"""
Created at: Jun, 2023
Last edit: Jun, 2023
author: Gustavo B. Barbosa (gustavo.bort.barbosa@gmail.com)
"""
from datetime import datetime
import pandas as pd
import configparser
import arcgis
import os

class ConnectGIS():
    """
    Connect to GIS Portal Enrteprise on PD_Publisher login.
    
    It will be used to class 'GetData' and to class 'UploadData'.
    """
    def connect_to_gis(self) -> None:
        """
        Connect to gis server.
        """
                
        parser = configparser.ConfigParser()
        parser.read('pipeline.conf')

        url = parser.get('arcgis_portal', 'url')
        username = parser.get('arcgis_portal', 'username')
        password = parser.get('arcgis_portal', 'password')
        
        self.gis_login = arcgis.GIS(
            url = url,
            username = username,
            password = password
            )

        return self.gis_login
    
class GetData(ConnectGIS):
    """
    Import data from Portal GIS, associated with the login account defined in ConnectGIS.
    
    This class extends ConnectGIs to establish a connection and retrieve survey data.
    
    Attributes:
        id_form (str): The form ID of the Feature Layer. This information is available on Portal GIS.
        prefix (str): The project prefix used to identify all related files.
        save_directory (str): The directory to save the CSV file.
        survey_by_id: Data retrieved from the GIS survey by ID.

    Methods:
        __init__(id_form, prefix, save_directory): Initializes the GetData object.
        get_main_layer(save=False, file_name=None): Import main layer data.
        get_table(table_position, save=False, file_name=None): Import main layer data.
    """

    def __init__(self, id_form, save_directory, prefix=None) -> None:
        """
        Initialize the GetData object.

        Args:
            id_form (str): form id of the Feature Layer. This information is available on Portal GIS.
            save_directory (str, optional): directory to save csv file.
            prefix (str): project prefix to identify all files.

        Returns:
            None.
        """
        super().__init__()

        self.id_form = id_form
        self.prefix = prefix
        self.save_directory = save_directory
        
        self.connect_to_gis()
        self.survey_by_id = self.gis_login.content.get(self.id_form)
        print(f'\nForm to download: {self.survey_by_id}')
        
    def get_main_layer(self, layer_position: int, save=False, file_name=None) -> pd.DataFrame:
        """
        Import main layer data.

        Args:
            layer_position (int): position of the layer to be imported.
            save (boolean, optional): if True, create a csv file. Defaults to False.
            file_name (str, optional): name of the csv output file.

        Returns:
            main_layer (DataFrame): main layer in Pandas Dataframe.
            main_layer (csv): main layer file in csv format.

        """
        main_layer = self.survey_by_id.layers[layer_position]
        self.df_main = main_layer.query(as_df=True)
        
        if save==True:
            if self.prefix==None:
                file = os.path.join(self.save_directory, (file_name + '.csv'))
                self.df_main.to_csv(file, index=False, decimal=',', sep=';')
                return self.df_main
            else:
                file = os.path.join(self.save_directory, (self.prefix + file_name + '.csv'))
                self.df_main.to_csv(file, index=False, decimal=',', sep=';')
                return self.df_main
        else:
            return self.df_main
        
    def get_table(self, table_position: int, save=False, file_name=None) -> pd.DataFrame:
        """
        Import form table's data.

        Args:
            table_position (int): position of the table to be imported.
            save (boolean, optional): if True, create a csv file. Defaults to False.
            file_name (str, optional): name of the csv output file.

        Returns:
            table (str): form's table.

        """
        rel_table = self.survey_by_id.tables[table_position]
        self.df_table = rel_table.query(as_df=True)
        
        if save==True:
            if self.prefix==None:
                file = os.path.join(self.save_directory, (file_name + '.csv'))
                self.df_table.to_csv(file, index=False, decimal=',', sep=';')
                return self.df_table
            else:
                file = os.path.join(self.save_directory, (self.prefix + file_name + '.csv'))
                self.df_table.to_csv(file, index=False, decimal=',', sep=';')
                return self.df_table
        else:
            return self.df_table
    
class UploadToPortal(ConnectGIS):
    """
    Handles the uploading of a CSV file to a folder on Portal GIS, 
    associated with the login account defined in ConnectGIS.

    To ensure the success of the updated process, it must have already been uploaded to the Portal 
    and have the same data structure

    Attributes:
        upload_directory (str): the directory where the file to be uploaded is located.
        file_format (str): format of the file. Default is 'CSV'.

    Methods:
        __init__(upload_directory): Initialize the UploadToPortal object and define the directory.
        upload_to_portal(file_name, file_format): Uploads a file to the Portal.
    """

    def __init__(self, upload_directory) -> None:
        """
        Initialize the UploadToPortal object.

        Args:
            upload_directory (str): directory where the file to be uploaded is located.

        Returns:
            None.
        """
        super().__init__()

        self.upload_directory = upload_directory

    def upload_to_portal(self, file_name, file_format='CSV') -> None:
        """
        Upload file defined on 'file_name' from directory defined on 'upload_directory'.

        Args:
            file_name (str): name of the csv output file.
            file_format (str, optional): format file to be uploaded. Defaults to 'CSV'.

        Returns:
            None.
        """
        self.connect_to_gis()
        
        data_portal = self.gis_login.content.search(
            query=file_name,
            item_type=file_format
            )[0]
        
        file = os.path.join(self.upload_directory, (file_name + '.csv'))

        if data_portal.update(data=file)==True:
            print("\nFile {} successfully updated to Portal".format(file_name))
        else:
            print("\nERROR: File {} not saved on Portal".format(file_name))

class GetPhotos(ConnectGIS):
    
    def __init__(self, id_form, save_directory='./photos/', folder_name=None):
        """
        Initialize the objects.

        Args:
            id_form (str): The form ID of the Feature Layer. This information is available on Portal GIS.
            save_directory (str, optional): Main directory to save photos. Defaults to './photos/'.
            folder_name (str, optional): Folder to save photos inside the "save_directory". Defaults to None.

        Returns:
            None.
        """
        super().__init__()
        
        self.id_form = id_form
        self.save_directory = save_directory
        self.folder_name = folder_name
        
    def get_photos(self) -> None:
        """
        This function checks if a log file (log_photos) has been created. 
        If it has, the function retrieves the maximum value for objectid (max_oid) from the log file 
        and downloads the photos where the objectid is greater than "max_oid," updating the log file afterward. 
        If the log file hasn't been created, it downloads all the photos and creates a log file.

        Args:
            None.

        Returns:
            photos (jpeg): photos saved on folder defined.
        """
        # Get log informations
        log_file = os.path.join(self.save_directory, 'log_photos.json')
        
        if os.path.exists(log_file) == True:
            df_log_previous = pd.read_json(log_file)
            max_oid = df_log_previous['objectid'].max()
            
        else:
            max_oid = 0
            print('\n[ATTENTION] Log file "log_photos.json" not found. All photos will be downloaded.')
        
        # Connect to GIS
        self.connect_to_gis()
        
        # Get form infos
        survey_by_id = self.gis_login.content.get(self.id_form)
        rel_fs = survey_by_id.layers + survey_by_id.tables

        print(f'\nForm to download photos: {survey_by_id}')

        # Define a dictionary with the new values        
        new_objects_control = {
            'objectid': [],
            'created_date': [], 
            'id_form': [], 
            'local': []
            }

        for indice_fs, fs in enumerate(rel_fs):
            # Check if the layer or table has any attachments
            if fs.properties.hasAttachments == True:
    
                feature_object_ids = fs.query(
                    out_fields="created_date",
                    where = f'objectid > {max_oid}', 
                    return_geometry=False,
                    order_by_fields='objectid ASC'
                    )
    
                if len(feature_object_ids.features) == 0:
                    print('\n[ATTENTION] There is no new photos to be downloaded.\nCheck the last "objectid" on log file.')
                
                else:
                    # Create log for the download
                    for indice, feature in enumerate(feature_object_ids.features):
            
                        print(f'Downloading photo: {indice + 1}/{len(feature_object_ids.features)}')
                        
                        # Create final folder
                        folder_date = datetime.utcfromtimestamp(feature.attributes['created_date']/1000).strftime("%y-%m-%d")
                        final_folder = os.path.join(self.save_directory, self.folder_name, folder_date)
                        
                        # Check if the folder exists. If not, then it is created.
                        if os.path.exists(final_folder) == False:
                            os.makedirs(final_folder)
                                            
                        new_objects_control['objectid'].append(feature.attributes['objectid'])
                        new_objects_control['created_date'].append(feature.attributes['created_date'])
                        new_objects_control['id_form'].append(self.id_form)
                        new_objects_control['local'].append(final_folder)
                
                        # Download the photos defined on the objectid list
                        object_oid = feature.attributes['objectid']
                        object_oid_attachments = fs.attachments.get_list(oid=object_oid)
                    
                        if len(object_oid_attachments) > 0:
                            for k in range(len(object_oid_attachments)):
                                attachment_id = object_oid_attachments[k]['id']
                                
                                object_attachment_path = fs.attachments.download(
                                    oid = object_oid, 
                                    attachment_id = attachment_id, 
                                    save_path = final_folder
                                    )
                                
                                # Add the objectid on the final name of the file
                                final_filename = object_attachment_path[0].replace(
                                    '.jpg', 
                                    f'-oid_{object_oid}.jpg'
                                    )

                                if os.path.exists(final_filename) == False:
                                    os.rename(
                                        object_attachment_path[0], 
                                        final_filename
                                        )
                                else:
                                    print(f'[EXCEPTION] File with objectid= {object_oid} was not downloaded because the file already exists\
                                          \nFile name:"{final_filename}"')

        # Create a log dataframe
        df_log_new = pd.DataFrame(new_objects_control)
        
        # Format the date
        df_log_new['created_date'] = pd.to_datetime(df_log_new['created_date'], unit='ms')
        df_log_new['created_date'] = df_log_new['created_date'].dt.strftime('%d/%m/%Y %H:%M:%S')
        
        # Append new data to previous data
        if os.path.exists(log_file) == True:
            df_log_new = pd.concat([df_log_previous, df_log_new], ignore_index=True)
        
        # Save to json
        df_log_new.to_json(log_file)
        
        print('\nAll the photos were downloaded.')

## PRÓXIMOS PASSOS:
# 1 - Criar class para download incremental na AWS (aproveitar código existente)
# 2 - Ajustar class GetPhotos para salvar photos na AWS
