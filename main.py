"""Description"""
"""
    A script used to create 3 csv files as per a config file. 
    Retrieving object and field metadata from salesforce across multiple orgs,objects and fields
"""
"""Requirements"""
import logging
import os
import traceback
import time
import json
from datetime import datetime
import cProfile
import pstats

import pandas as pd
from simple_salesforce import Salesforce
from simple_salesforce.exceptions import SalesforceAuthenticationFailed
from typing import List, Dict, Tuple, Any

"""Classes"""
class RunStats:
    def __init__(self):
        self.duration = 0
        self.org_count = 0
        self.successrate = 0
        self.success_count = 0
        self.failed_orgs = []
        self.failed_objects = []
        self.orgs = []
        
    class Org:
        def __init__(self, name):
            self.name = name
            self.duration = 0
            self.object_count = 0
            self.successrate = 0
            self.success_count = 0
            self.objects = []
            
        class Object:
            def __init__(self, name):
                self.name = name
                self.duration = 0
                
        def add_object(self, object_name):
            object_stats = self.Object(object_name)
            self.objects.append(object_stats)
            return object_stats
        
    def add_org(self, org_name):
        org_stats = self.Org(org_name)
        self.orgs.append(org_stats)
        return org_stats

"""Constants"""
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPT_PATH = os.path.basename(__file__)
DATE_TIME_FORMAT = "%Y-%m-%dT%H-%M-%S"
CONFIG_FILE_PATH = os.path.join(SCRIPT_DIR, 'config.json')
 
def datetime_to_excel_serial(date_time: datetime) -> float:
    """Converts a datetime object to Excel's serial date format."""
    EPOCH_DATE = datetime(1900, 1, 1)
    delta = date_time - EPOCH_DATE
    return (delta.days + (delta.seconds / 86400)) + 2  # Add 2 to account for Excel's leap year bug

VALUE_START_FORMATTED_DATETIME = datetime_to_excel_serial(datetime.now())
FILE_START_FORMATTED_DATETIME = datetime.now().strftime(DATE_TIME_FORMAT)

"""Setup Functions"""
def is_valid_path(path: str) -> bool:
    """Check whether a given string is a valid path."""
    return os.path.exists(path)

def load_config(config_file_path: str) -> dict:
    """Load from config_file_path config.json data from a JSON file."""
    try:
        with open(config_file_path, 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file not found at path: {config_file_path}")
    except json.JSONDecodeError:
        raise json.JSONDecodeError(f"Invalid JSON in configuration file at path: {config_file_path}")
    return config

def load_object_list(object_list: List) -> List:
    """Returns a sorted and unique object list."""
    logging.info(f"object list before load: {object_list}")
    sorted_object_list = sorted(set(object_list))
    logging.info(f"object list after loading: {sorted_object_list}")
    return sorted_object_list

def setup_logger(datetime_str: str, config: dict) -> logging.Logger:
    """Set up logging for the script based on the config.json"""
    create_logfile = config.get('create_logfile', False)
    log = logging.getLogger(__name__)
    log.handlers = []
    log_dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'script_logs')
    os.makedirs(log_dir_path, exist_ok=True)
    log_file_path = os.path.join(log_dir_path, f'logfile_{datetime_str}.log')
    console_level = getattr(logging, config.get('console_level', 'INFO').upper())
    file_level = getattr(logging, config.get('file_level', 'DEBUG').upper())
    formatter = logging.Formatter(
        '%(asctime)s %(levelname)s %(message)s',
        DATE_TIME_FORMAT
    )
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level)
    console_handler.setFormatter(formatter)
    console_handler.set_name('console')
    console_handler.__docstring__ = 'Send log messages to console'
    log.addHandler(console_handler)
    log.propagate = False
    if create_logfile :
        file_handler = logging.FileHandler(log_file_path)
        file_handler.setLevel(file_level)
        file_handler.setFormatter(formatter)
        file_handler.set_name('file')
        file_handler.__docstring__ = f'Send log messages to file {log_file_path}'
        log.addHandler(file_handler)
        log.propagate = False
    return log

"""Variables"""
script_dateTime = time.strftime(DATE_TIME_FORMAT, time.localtime())

# Load configuration data and setup logging
CONFIG = load_config(CONFIG_FILE_PATH)
LOGGER = setup_logger(datetime.now().strftime(DATE_TIME_FORMAT), CONFIG['logging'])
LOGGER.setLevel(logging.DEBUG)

"""Functions"""
def generate_csv_full_filepath(file_name: str, date_time: str) -> str:
    """Generates a new filename based on the original filename and date/time."""
    new_file_name = f"{os.path.splitext(os.path.basename(file_name))[0]}_{date_time}.csv"
    script_output_dir = os.path.join(SCRIPT_DIR, 'script_output')
    if not os.path.exists(script_output_dir):
        os.mkdir(script_output_dir)
    return os.path.join(script_output_dir, new_file_name)

def determine_field_ui_type(api_type: str) -> str:
    """Determines the UI type from the API type property of a Salesforce field."""
    UI_TYPE_MAPPING = {
        'address': 'Address',
        'anyType': 'Text',
        'base64': 'Text',
        'boolean': 'Checkbox',
        'combobox': 'Picklist',
        'complexvalue': 'Text',
        'currency': 'Currency',
        'datacategorygroupreference': 'Text',
        'date': 'Date',
        'datetime': 'Datetime',
        'double': 'Number',
        'email': 'Email',
        'encryptedstring': 'Text',
        'id': 'Lookup',
        'int': 'Number',
        'junctionIdList': 'Text',
        'junctionReferenceTo': 'Text',
        'location': 'Location',
        'long': 'Number',
        'masterrecord': 'Master-Detail',
        'multipicklist': 'Multi-Select Picklist',
        'percent': 'Percent',
        'phone': 'Phone',
        'picklist': 'Picklist',
        'reference': 'Lookup',
        'string': 'Text',
        'textarea': 'Long Text Area',
        'time': 'Time',
        'url': 'URL',
        'encryptedtext': 'Encrypted Text',
        'encryptedemail': 'Encrypted Email',
        'encryptedphone': 'Encrypted Phone',
        'encryptedurl': 'Encrypted URL',
        'richtextarea': 'Rich Text Area',
        'html': 'HTML',
        'file': 'File',
        'image': 'Image',
        'quickaction': 'Quick Action',
        'autonumber': 'Auto-Number'
    }
    ui_type = UI_TYPE_MAPPING.get(api_type, None)
    if ui_type is None:
        raise ValueError(f"Invalid API type: {api_type}")
    return ui_type

def determine_relationship_type(api_type: str) -> str:
    """Determines the relationship type based on the provided API type."""
    not_valid_relationship_type = "N/A"
    relationship_type_mapping = {
        'reference': 'Lookup',
        'lookup': 'Lookup',
        'master_detail': 'Master-Detail',
        'junctionIdList': 'Junction',
        'externalLookup': 'External Lookup',
        'indirectLookup': 'Indirect Lookup',
        'cascadeDelete': 'Cascade Delete',
        'rollupSummary': 'Rollup Summary'
    }
    if api_type not in relationship_type_mapping:
        return not_valid_relationship_type
    relationship_type = relationship_type_mapping[api_type]
    return relationship_type_mapping[api_type]

def determine_isFormula(calculated: bool) -> bool:
    """Determine if the field is a formula, based on its calculated property."""
    return calculated != ''

def get_object_record_type_ids(org_name: str, object_metadata: dict, header_columns: List[str]) -> pd.DataFrame:
    """Retrieve the record type IDs for a specified object from Salesforce metadata."""
    if 'recordTypeInfos' not in object_metadata:
        raise ValueError("Invalid object metadata: missing 'recordTypeInfos'")
    record_type_rows = []
    record_type_ids = {}
    for record_type in object_metadata['recordTypeInfos']:
        if not record_type['master']:
            developer_name = record_type['developerName']
            record_type_id = record_type['recordTypeId']
            record_type_ids[developer_name] = record_type_id
            record_type_row = {
                'date_synced_with_org': VALUE_START_FORMATTED_DATETIME,
                'custom_id': f"{org_name}{object_metadata['name']}{developer_name}",
                'org_name': org_name,
                'object_api_name': object_metadata['name'],
                'object_label': object_metadata['label'],
                'record_type_id': record_type_id,
                'record_type_name': developer_name
            }
            record_type_rows.append(record_type_row)
    try:
        df_record_type_ids = pd.DataFrame.from_dict(record_type_ids, orient='index', columns=['record_type_id'])
        df_record_type_ids.reset_index(inplace=True)
        df_record_type_ids.rename(columns={'index': 'record_type_name'}, inplace=True)
        df_record_type_ids['date_synced_with_org'] = VALUE_START_FORMATTED_DATETIME
        df_record_type_ids['custom_id'] = f"{org_name}{object_metadata['name']}"
        df_record_type_ids['org_name'] = org_name
        df_record_type_ids['object_api_name'] = object_metadata['name']
        df_record_type_ids['object_label'] = object_metadata['label']
        df_record_type_ids = df_record_type_ids[header_columns]
        return df_record_type_ids
    except Exception as e:
        LOGGER.exception(f"get_object_record_type_ids: Error creating DataFrame for object {object_metadata['name']}: {e}")
        raise

def get_object_picklist_values(sf,org_name, object_metadata, header_columns)-> pd.DataFrame:
    """Returns a DataFrame containing the picklist values for all picklist fields on the given object."""
    fields = object_metadata['fields']
    picklist_fields = [field for field in fields if field['type'] == 'picklist']
    if not picklist_fields:
        # If there are no picklist fields, return an empty DataFrame
        return pd.DataFrame(columns=header_columns)
    object_api_name = object_metadata['name']
    object_label = object_metadata['label']
    picklist_values = []
    for field in picklist_fields:
        field_api_name = field['name']
        field_label = field['label']
        field_api_type = field['type']
        field_metadata = next((f for f in sf.__getattr__(object_api_name).describe()['fields'] if f['name'] == field['name']), None)
        for value in field_metadata['picklistValues']:
            row_dict = {
                'date_synced_with_org': VALUE_START_FORMATTED_DATETIME,
                'custom_id': f"{org_name}_{object_api_name}_{field_api_name}",
                'org_name': org_name,
                'object_api_name': object_api_name,
                'object_label': object_label,
                'field_api_name': field_api_name,
                'field_label': field_label,
                'field_ui_type': field_api_type,
                'field_is_required': not field.get('nillable', True),
                'field_is_restricted_picklist': field.get('restrictedPicklist', False),
                'field_is_formula': field.get('calculated', False),
                'active': value['active'],
                'default_value': value.get('defaultValue', False),
                'default_value_formula': value.get('defaultValueFormula', ''),
                'defaulted_on_create': value.get('defaultedOnCreate', False),
                'valid_for': value.get('validFor', []),
                'label': value['label'],
                'value': value['value'],
                'length': field.get('length', None),
                'precision': field.get('precision', None),
                'scale': field.get('scale', None),
                'validation_rules': field.get('validationRules', []),
                'dependant_fields': field.get('dependentPicklist', [])
            }
            picklist_values.append(row_dict)
    picklist_values_df = pd.DataFrame(picklist_values)
    picklist_values_df = picklist_values_df[header_columns]
    return picklist_values_df

def get_object_field_properties(org_name: str, object_metadata: dict, header_columns: pd.DataFrame) -> pd.DataFrame:
    """Retrieves field information for a Salesforce object and adds it to a Pandas DataFrame."""   
    object_api_name = object_metadata['name']
    object_label = object_metadata['label']
    fields = object_metadata['fields']
    field_list = []
    for field in fields:
        field_api_name = field['name']
        field_label = field['label']
        field_api_type = field['type']
        field_dict = {
            'date_synced_with_org': VALUE_START_FORMATTED_DATETIME,
            'custom_id': f'{org_name}_{object_api_name}_{field_api_name}',
            'org_name': org_name,
            'object_api_name': object_api_name,
            'object_label': object_label,
            'field_api_name': field_api_name,
            'field_label': field_label,
            'field_ui_type': determine_field_ui_type(field_api_type),
            'field_api_type': field_api_type,
            'field_relationship_type': determine_relationship_type(field_api_type),
            'parent_object_api_name': field.get('referenceTo', None),
            'field_is_required': not field.get('nillable', True),
            'field_is_restricted_picklist': field.get('restrictedPicklist', False),
            'field_is_unique': field.get('unique', False),
            'field_is_formula': field.get('calculated', False),
            'default_value': field.get('defaultValue', None),
            'default_value_formula': field.get('defaultValueFormula', None),
            'defaulted_on_create': field.get('defaultedOnCreate', None),
            'length': field.get('length', None),
            'precision': field.get('precision', None),
            'scale': field.get('scale', None),
            'validation_rules': field.get('validationRules', None),
            'dependant_fields': field.get('dependentPicklist', None),
        }
        field_list.append(field_dict)
    df_field_properties = pd.DataFrame(field_list)
    df_field_properties = df_field_properties[header_columns]
    return df_field_properties

def get_salesforce_instance(sf_org_credentials: dict) -> Salesforce:
    """Authenticate a Salesforce user using the provided credentials."""
    sf_instance = {}
    try:
        # Use the provided credentials to authenticate with the Salesforce org
        sf_instance = Salesforce(
            username=sf_org_credentials['user_name'],
            password=sf_org_credentials['user_password'],
            security_token=sf_org_credentials['user_token'],
            domain=sf_org_credentials['domain']
        )
    except SalesforceAuthenticationFailed:
        LOGGER.warning('Invalid Salesforce credentials')
        #raise ValueError('Invalid Salesforce credentials')

    except Exception as e:
        LOGGER.error(f'Failed to connect to Salesforce org: {str(e)}')
        #raise ConnectionError(f'Failed to connect to Salesforce org: {str(e)}')
    return sf_instance

def retrieve_object_metadata(salesforce_instance, object_name)-> dict:

    """Retrieves the object metadata from the Salesforce instance."""
    object_metadata = {}
    try:
        object_metadata = salesforce_instance.__getattr__(object_name).describe()
        return object_metadata
    except Exception as e:
        return object_metadata
    
def get_org_and_object_lists(config: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    """Returns the org and object lists based on the run_type variable in the config."""
    org_list: List[str] = []
    object_list: List[str] = []
    if config.get("run_type") == "test":
        org_list = list(config.get("orgs").get("test_org"))
        unsorted_object_list = list(config.get("objects").get("test_objects"))
        object_list = load_object_list(unsorted_object_list)
    else:
        org_list = list(config.get("orgs").get("real_orgs"))
        unsorted_object_list = [obj for obj_list in config.get("objects").get("real_objects").values() for obj in obj_list]
        object_list = load_object_list(unsorted_object_list)
    return org_list, object_list

def get_org_credentials(org: str, config: Dict[str, Any]) -> dict:
    """Returns the org credential list based on the org and run_type variable in the config."""
    credentials: dict = {}
    if config.get("run_type") == "test":
        credentials = config.get("orgs").get("test_org").get(org)
    else:
        credentials = config.get("orgs").get("real_orgs").get(org)
    logging.info(f"credentials: {credentials}")
    return credentials

def export_csv(config: Dict)-> None:
    """Create and exports object properties, picklist values, and record type IDs to CSV files for specified orgs and objects."""
    # Constants
    RECORD_TYPE_HEADER = config["headers"]["csv_record_type_IDs_header"]
    PICKLIST_VALUES_HEADER = config["headers"]["csv_picklist_values_header"]
    FIELD_PROPERTIES_HEADER = config["headers"]["csv_field_properties_header"]

    # DataFrames
    record_type_ids_df = pd.DataFrame(columns=RECORD_TYPE_HEADER)
    field_picklist_values_df = pd.DataFrame(columns=PICKLIST_VALUES_HEADER)
    object_field_properties_df = pd.DataFrame(columns=FIELD_PROPERTIES_HEADER)

    #fetch org and object list from config
    ORG_LIST, OBJECT_LIST = get_org_and_object_lists(config)
    LOGGER.info(f"org_list: {ORG_LIST}")
    LOGGER.info(f"object_list: {OBJECT_LIST}")

    #setup run_stats
    run_stats = RunStats()
    run_stats.org_count = 0

    number_of_orgs = len(ORG_LIST)
    number_of_org_objects = len(OBJECT_LIST)
    number_of_total_objects = len(ORG_LIST) * len(OBJECT_LIST)

    for org_name in ORG_LIST:

        #setup org_stats
        org_stats = run_stats.add_org(org_name)
        org_stats.object_count = 0
        run_stats.org_count += 1

        LOGGER.info(f"processing org {number_of_orgs}/{run_stats.org_count} org:{org_name}.")
        LOGGER.info(f"connecting to {org_name}...")
        
        #connect to org instance
        org_credentials = get_org_credentials(org_name, config)
        salesforce_instance = get_salesforce_instance(org_credentials)
        
        if salesforce_instance: 
            LOGGER.info(f"connected to: {org_name}")
            LOGGER.info(f"processing objects...")

            for object_name in OBJECT_LIST:

                #setup objects_stats
                object_stats = org_stats.add_object(object_name)
                object_start_time = time.time()
                org_stats.object_count += 1

                #get object metadata
                object_metadata = retrieve_object_metadata(salesforce_instance, object_name)

                if object_metadata:
                    try:
                        #append new rows for each dataframe
                        record_type_ids_df = pd.concat([record_type_ids_df, get_object_record_type_ids(org_name, object_metadata, RECORD_TYPE_HEADER)], ignore_index=True)

                        field_picklist_values_df = pd.concat([field_picklist_values_df, get_object_picklist_values(salesforce_instance, org_name, object_metadata, PICKLIST_VALUES_HEADER)], ignore_index=True)

                        object_field_properties_df = pd.concat([object_field_properties_df, get_object_field_properties(org_name, object_metadata, FIELD_PROPERTIES_HEADER)], ignore_index=True)
                        
                        #get process duration
                        call_duration = time.time() - object_start_time

                        #get object stats
                        object_stats.duration = call_duration

                        #get org stats
                        org_stats.duration += call_duration
                        org_stats.success_count += 1
                        org_stats.successrate = org_stats.success_count/number_of_org_objects

                        #get run stats
                        run_stats.duration += call_duration
                        LOGGER.info(f"retrieved object: {org_name}_{object_name} {round(object_stats.duration, 2)}sec")

                    except Exception as e:
                        LOGGER.exception(f"error processing object {org_name}_{object_name} : {str(e)}")
                        continue    
                else:
                    run_stats.failed_objects.append(f"{org_name}_{object_name}")
                    LOGGER.info(f"error processing object {org_name}_{object_name}")
            run_stats.success_count += 1
            run_stats.successrate = run_stats.success_count/number_of_orgs    
        else:
            run_stats.failed_orgs.append(org_name)

        LOGGER.info(f"org processed: {org_name} success: {org_stats.successrate * 100}% duration: {round(org_stats.duration, 2)} seconds")    
            
        # Generate the full file paths
        record_type_ids_file = generate_csv_full_filepath("record_type_ids.csv", FILE_START_FORMATTED_DATETIME)
        field_picklist_values_file = generate_csv_full_filepath("field_picklist_values.csv", FILE_START_FORMATTED_DATETIME)
        object_field_properties_file = generate_csv_full_filepath("object_field_properties.csv", FILE_START_FORMATTED_DATETIME)

        # Export the dataframes to CSV files
        try:
            record_type_ids_df.to_csv(record_type_ids_file, index=False)
            field_picklist_values_df.to_csv(field_picklist_values_file, index=False)
            object_field_properties_df.to_csv(object_field_properties_file, index=False)
        except Exception as e:
                LOGGER.exception(f"Error exporting CSVs: {str(e)}")

    return run_stats
    
"""main function"""
def main() -> None:
    """Main function."""
    # Log script execution start and configuration data.
    LOGGER.info("Script execution started.")

    # Run the script.
    try:
        this_runs_stats = export_csv(CONFIG)
        if len(this_runs_stats.failed_orgs) > 0: LOGGER.info(f"failed orgs: {this_runs_stats.failed_orgs}")
        if len(this_runs_stats.failed_objects) > 0: LOGGER.info(f"failed objects: {this_runs_stats.failed_objects}")
        LOGGER.info(f"run completed: success: {this_runs_stats.successrate * 100}% duration: {round(this_runs_stats.duration, 2)} seconds")

    except Exception as e:
        # Log exceptions with traceback.
        LOGGER.exception("Error occurred: {}".format(str(e)))
        traceback.print_exc()

    # Log script execution completion.
    LOGGER.info("Script execution completed.")

if __name__ == '__main__':
    # Create the profiler stats directory if it doesn't exist
    stats_dir = os.path.join(os.path.dirname(__file__), 'profiler_stats')
    if not os.path.exists(stats_dir):
        os.mkdir(stats_dir)

    # Run the profiler and write the output to a binary file
    profile_filename = os.path.join(stats_dir, 'profile_output.bin')
    cProfile.run("main()", filename=profile_filename)

    # Parse the profiler output and print it to a text file
    output_filename = os.path.join(stats_dir, 'profile_output.txt')
    with open(output_filename, 'w') as f:
        stats = pstats.Stats(profile_filename, stream=f)
        stats.sort_stats('cumulative').print_stats()
