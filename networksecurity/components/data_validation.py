# Data validation is not very useful in this project. As the train and test both files are being spliited from the same dataset.
# Its only done as a form of practice to show how data validation can be implemented in a machine learning pipeline.
# Data validation is useful in case of real time data is being ingested from a source like a database or an API.(sentiment analysis, stock market prediction, etc.)
# Some of the real world example of data validation are Algorithmic Trading Firms and High Frequency Trading Firms.

from networksecurity.entity.artifact_entity import DataValidationArtifact, DataIngestionArtifact
from networksecurity.entity.config_entity import DataValidationConfig
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from networksecurity.constant.training_pipeline import SCHEMA_FILE_PATH
from networksecurity.utils.main_utils.utils import read_yaml_file, write_yaml_file

from scipy.stats import ks_2samp
import pandas as pd
import os
import sys

class DataValidation:
    def __init__(self,data_ingestion_artifact:DataIngestionArtifact,
                 data_validation_config:DataValidationConfig):
        
        try:
            self.data_ingestion_artifact=data_ingestion_artifact
            self.data_validation_config=data_validation_config
            self._schema_config = read_yaml_file(SCHEMA_FILE_PATH)
        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
    def validate_number_of_columns(self, dataframe: pd.DataFrame) -> bool:
        try:
            number_of_columns=len(self._schema_config["columns"])
            logging.info(f"Required number of columns:{number_of_columns}")
            logging.info(f"Data frame has columns:{len(dataframe.columns)}")
            if len(dataframe.columns)==number_of_columns:
                return True
            return False
        except Exception as e:
            raise NetworkSecurityException(e, sys)
        
    def detect_dataset_drift(self,base_df,current_df,threshold=0.05)->bool:
        try:
            status=True
            report={}
            for column in base_df.columns:
                d1=base_df[column]
                d2=current_df[column]
                is_same_dist=ks_2samp(d1,d2)
                if threshold<=is_same_dist.pvalue:
                    is_found=False
                else:
                    is_found=True
                    status=False
                report.update({column:{
                    "p_value":float(is_same_dist.pvalue),
                    "drift_status":is_found
                    
                    }})
            drift_report_file_path = self.data_validation_config.drift_report_file_path

            #Create directory
            dir_path = os.path.dirname(drift_report_file_path)
            os.makedirs(dir_path,exist_ok=True)
            write_yaml_file(file_path=drift_report_file_path,content=report)

            return status
            

        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
    def initiate_data_validation(self)->DataValidationArtifact:
        try:
            train_file_path, test_file_path = self.data_ingestion_artifact.trained_file_path, self.data_ingestion_artifact.test_file_path

            train_dataframe = pd.read_csv(train_file_path)
            test_dataframe = pd.read_csv(test_file_path)

            status_train = self.validate_number_of_columns(train_dataframe)
            if not status_train:
                logging.info("Train dataset validation failed.")

            status_test = self.validate_number_of_columns(dataframe=test_dataframe)
            if not status_test:
                logging.info("Test dataset validation failed.")

            status_drift=self.detect_dataset_drift(base_df=train_dataframe,current_df=test_dataframe)
            if not status_drift:
                logging.info("Dataset drift detected between train and test datasets.")

            status = status_train and status_test and status_drift

            if status:
                dir_path=os.path.dirname(self.data_validation_config.valid_train_file_path)
                os.makedirs(dir_path,exist_ok=True)

                train_dataframe.to_csv(self.data_validation_config.valid_train_file_path, index=False, header=True)
                test_dataframe.to_csv(self.data_validation_config.valid_test_file_path, index=False, header=True)
                
                data_validation_artifact = DataValidationArtifact(
                    validation_status=status,
                    valid_train_file_path=self.data_validation_config.valid_train_file_path,
                    valid_test_file_path=self.data_validation_config.valid_test_file_path,
                    invalid_train_file_path=None,
                    invalid_test_file_path=None,
                    drift_report_file_path=self.data_validation_config.drift_report_file_path,
                )
                return data_validation_artifact
            else:
                dir_path=os.path.dirname(self.data_validation_config.invalid_train_file_path)
                os.makedirs(dir_path,exist_ok=True)

                train_dataframe.to_csv(self.data_validation_config.invalid_train_file_path, index=False, header=True)
                test_dataframe.to_csv(self.data_validation_config.invalid_test_file_path, index=False, header=True)
                
                data_validation_artifact = DataValidationArtifact(
                    validation_status=status,
                    valid_train_file_path=None,
                    valid_test_file_path=None,
                    invalid_train_file_path=self.data_validation_config.invalid_train_file_path,
                    invalid_test_file_path=self.data_validation_config.invalid_test_file_path,
                    drift_report_file_path=self.data_validation_config.drift_report_file_path,
                )
                return data_validation_artifact

        except Exception as e:
            raise NetworkSecurityException(e, sys)

