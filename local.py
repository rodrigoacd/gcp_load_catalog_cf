from google.cloud import datacatalog_v1
from google.cloud import datacatalog

import pandas as pd
import gcsfs

project_id = "bch-prj-bdta-pocarq-dev-41f1"
dataset_id = "demo_dataset"
tag_template_id = "bch_gdd_universal_campo"
location = "us-east4"

bucketName = "stefanini_poc_datacatalog/bch-prj-bdta-pocarq-dev-41f1"
fileName =   "column_catalog.csv"

datacatalog_client = datacatalog_v1.DataCatalogClient()
# print("start cloud function")
# def hello_gcs(event, context):
def catalog():

    # bucketName = event['bucket'];
    # fileName = event['name'];
    print (f"gs://{ bucketName }/{ fileName }")
    fs = gcsfs.GCSFileSystem(project=project_id, mode="r")
    with fs.open(f"gs://{ bucketName }/{ fileName }") as f:
        df = pd.read_csv(f, delimiter=';',on_bad_lines='skip')

    for index, row in df.iterrows():
        # Lookup Data Catalog's Entry referring to the table.
        resource_name = (
            f"//bigquery.googleapis.com/projects/{project_id}"
            f"/datasets/{row['dataset']}/tables/{row['table']}"
        )
        table_entry = datacatalog_client.lookup_entry(
            request={"linked_resource": resource_name}
        )
        #print(table_entry.name)
        #print(row['column'])
        try:    
            tag_exists, tag_id = check_if_exists(parent=table_entry.name, column=row['column'])
            #print('tag exists: ', tag_exists)
            print('tag id: ', tag_id)
        except Exception as e:
            print('Error during check_if_exists: ', e)
            creation_status = "error"
            return creation_status        
        

        tag = datacatalog_v1.types.Tag()

        tag.template = f"projects/{project_id}/locations/{location}/tagTemplates/{tag_template_id}"
        tag.name = f"tag_{ row['column'] }"
        tag.column = row['column']

        try:
            template_path = datacatalog_client.tag_template_path(project_id, location, tag_template_id)
            tag_template = datacatalog_client.get_tag_template(name=template_path)
        except Exception as e:
                print(f"Template doesn't exists : { e }")


        for field_id, field_value in tag_template.fields.items():
            try:
                print(f"row[field_id] : {row[field_id]}")
                tag.fields[str(field_id)] = datacatalog_v1.types.TagField()
                tag.fields[str(field_id)].string_value = row[str(field_id)]
            except Exception as e:
                print(f"Error field_id doesn't exists : { e }")

        # for field in ['ADM', 'DOM', 'PRO', 'SUB', 'TEC']:
        #     tag.fields[field] = datacatalog_v1.types.TagField()
        #     tag.fields[field].string_value = row[field]

        # print(f"{tag}")

        if tag_exists:
            try:
                print(f"Tag Exists. Deleting: { tag.name }")
                request = datacatalog_v1.DeleteTagRequest(
                    name = tag_id
                )
                print(f"{ request }")
                response = datacatalog_client.delete_tag(request=request)
            except Exception as e:
                print(f"Error Deleting tag { e }")
        # Always execute the creation of tag       
        try:
            response = datacatalog_client.create_tag(parent=table_entry.name, tag=tag)
            print(f"Tag Created: { tag.name } ")
        except Exception as e:
            print(f"Error creating tag { e }")


def check_if_exists(parent, column=''):
    
    tag_exists = False
    tag_id = ""
    
    tag_list = datacatalog_client.list_tags(parent=parent, timeout=120)
    
    for tag_instance in tag_list:
        
        tagged_column = tag_instance.column
        
        template: f"projects/{project_id}/locations/{location}/tagTemplates/{tag_template_id}"
        
        tagged_template_project = tag_instance.template.split('/')[1]
        tagged_template_location = tag_instance.template.split('/')[3]
        tagged_template_id = tag_instance.template.split('/')[5]
        
        if column == "" or column == None:
            # looking for a table-level tag
            if tagged_template_id == tag_template_id and tagged_template_project == project_id and \
                tagged_template_location == location and tagged_column == "":
                #print('DEBUG: Table tag exists.')
                tag_exists = True
                tag_id = tag_instance.name
                #print('DEBUG: tag_id: ' + tag_id)
                break
        else:
            # looking for a column-level tag
            if column == tagged_column and tagged_template_id == tag_template_id and tagged_template_project == project_id and \
                tagged_template_location == location:
                #print('Column tag exists.')
                tag_exists = True
                tag_id = tag_instance.name
                #print('DEBUG: tag_id: ' + tag_id)
                break
       
    return tag_exists, tag_id
catalog()