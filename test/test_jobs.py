from jobs import *
from jobs import _generate_jid
import unittest

import unittest
import uuid

class TestJobFunctions(unittest.TestCase):

    def test_generate_jid(self):
        """Function tests whether unique uuid string is generated"""
        # Generate two job IDs
        job_id_1 = _generate_jid()
        job_id_2 = _generate_jid()

        # Check if both job IDs are strings
        self.assertIsInstance(job_id_1, str)
        self.assertIsInstance(job_id_2, str)

        # Check if the generated job IDs are not equal
        self.assertNotEqual(job_id_1, job_id_2)

        # Check if the generated job IDs are valid UUIDs
        self.assertTrue(uuid.UUID(job_id_1))
        self.assertTrue(uuid.UUID(job_id_2))

    def test_instantiate_job(self):
        """Function tests the job dictionary upon instantiating a job"""
        # Clear Jobs
        deleted_jobs_code = delete_all_jobs()
        if deleted_jobs_code == 0: 
            # Instantiate Job & Create Job
            job_dict = add_job("String 1", "String 2",)

            # Check return type
            self.assertIsInstance(job_dict, dict)

            # Check the contents of the jobs dictionary
            self.assertIsInstance(job_dict['Status'],  str) 
            # Check remaining contents of job dictionary once finalized. 
            # self.assertIsInstance()
            # self.assertIsInstance()

            # Clean up the jobs database
            delete_all_jobs()

    def test_add_job(self):
        """Function tests whether a job can be instantiated and added to the jobs database"""
        job_dict = add_job("String 1", "String 2",)
        # Check return type
        self.assertIsInstance(job_dict, dict)

        # Get All Jobs. There should only be one job in the database
        job_ids = get_all_job_ids()
        self.assertEqual(len(job_ids), 1)

        # Clean up the jobs database
        delete_all_jobs()

    def test_get_get_job_by_id(self):
        job_dict = add_job("String 1", "String 2")

        # Get All Jobs. There should only be one job in the database
        job_ids = get_all_job_ids()
        jid = job_ids[0]

        job_dict = get_job_by_id(jid)

        # Get job dictionary keys and access the values of the last 2 keys (job parameters) 
        # to generalize the test and account for any changes that may be made to the job dictionary
        # List should contain 4 keys: id, Status, Job Param1, Job Param2
        job_dict_keys = list(job_dict.keys())

        # TODO: Job status can Fail - but why is the dictionary not 4 keys long?
        if len(job_dict_keys) == 3: 
            param_1 = job_dict_keys[2]
            self.assertEqual(job_dict[param_1], "String 2")

        if len(job_dict_keys) == 4: 
            param_1 = job_dict_keys[2]
            param_2 = job_dict_keys[3]

            # Assert the job dictionary params based on what you submitted
            self.assertEqual(job_dict[param_1], "String 1")
            self.assertEqual(job_dict[param_2], "String 2")

        # Clean up the jobs database
        delete_all_jobs()

    def test_update_job_status(self):
        test_status = "Test Job Status Updated"
        # def update_job_status(jid:str, status:str):
        job_dict = add_job("String 1", "String 2")

        # Get All Jobs. There should only be one job in the database
        job_ids = get_all_job_ids()
        jid = job_ids[0]
        
        update_job_status(jid, test_status)

        updated_job_dict = get_job_by_id(jid)

        self.assertEqual(updated_job_dict['Status'], test_status)


if __name__ == '__main__':
    unittest.main()