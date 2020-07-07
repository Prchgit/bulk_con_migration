# This file is the actual code for the Python runnable test
from dataiku.runnables import Runnable
from dataiku.customrecipe import *
import dataiku

class MyRunnable(Runnable):
    """The base interface for a Python runnable"""

    def __init__(self, project_key, config, plugin_config):
        """
        :param project_key: the project in which the runnable executes
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
        """
        self.project_key = project_key
        self.config = config
        self.plugin_config = plugin_config
        self.client = dataiku.api_client()
        
    def get_progress_target(self):
        """
        If the runnable will return some progress info, have this function return a tuple of 
        (target, unit) where unit is one of: SIZE, FILES, RECORDS, NONE
        """
        return None

    def run(self, progress_callback):
        """
        Do stuff here. Can return a string or raise an exception.
        The progress_callback is a function expecting 1 value: current progress
        """
        Src_Connection  = self.config.get('Src_Connection')
        Dest_Connection = self.config.get('Dest_Connection')
        project         = self.client.get_project(self.project_key)
        conn            = 'connection'
       
        # check that source connection is valid
        try:
            self.client.get_connection(Src_Connection).get_info()
        except:
            raise Exception("Source connection does not exist or you do not have permission to view its details.") 
             
        # check that destination connection is valid    
        try:
            self.client.get_connection(Dest_Connection).get_info()
        except:
            raise Exception("Destination connection does not exist or you do not have permission to view its details.")
       
     
        datasets = project.list_datasets()
        migrated_datasets = []
        for i in datasets:
            myds = project.get_dataset(i['name'])
            myds_def = myds.get_definition()
            if(conn in myds_def['params'].keys() and myds_def['params']['connection'] == Src_Connection):
                try:
                    # try and execute if both connections are of same type(sql, hdfs, etc).
                    myds_def['params']['connection'] = Dest_Connection
                    myds.set_definition(myds_def)
                    migrated_datasets.append(i['name'])
                except:
                    raise Exception("Connection type mismatch")
                    
                
        return(migrated_datasets.values())
        