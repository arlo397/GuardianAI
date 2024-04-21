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
            self.assertIsInstance(type(job_dict['id']),  uuid.UUID) 
            self.assertIsInstance(type(job_dict['Status']),  str) 

            # Clean up the jobs database
            delete_all_jobs()

    def test_add_job(self):
        """Function tests whether a job can be instantiated and added to the jobs database"""
        # Clear Jobs
        deleted_jobs_code = delete_all_jobs()
        if deleted_jobs_code == 0: 
            # Instantiate Job  & Create Job
            job_dict = add_job("String 1", "String 2",)

            # Check return type
            self.assertIsInstance(job_dict, dict)

            # Get All Jobs. There should only be one job in the database
            job_ids = get_all_job_ids()
            self.assertEqual(len(job_ids), 1)

            # Clean up the jobs database
            delete_all_jobs()

if __name__ == '__main__':
    unittest.main()