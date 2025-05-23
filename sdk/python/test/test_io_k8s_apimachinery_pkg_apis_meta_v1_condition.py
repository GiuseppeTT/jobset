# coding: utf-8

"""
    JobSet SDK

    Python SDK for the JobSet API

    The version of the OpenAPI document: v0.1.4
    Generated by OpenAPI Generator (https://openapi-generator.tech)

    Do not edit the class manually.
"""  # noqa: E501


import unittest

from jobset.models.io_k8s_apimachinery_pkg_apis_meta_v1_condition import IoK8sApimachineryPkgApisMetaV1Condition

class TestIoK8sApimachineryPkgApisMetaV1Condition(unittest.TestCase):
    """IoK8sApimachineryPkgApisMetaV1Condition unit test stubs"""

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def make_instance(self, include_optional) -> IoK8sApimachineryPkgApisMetaV1Condition:
        """Test IoK8sApimachineryPkgApisMetaV1Condition
            include_optional is a boolean, when False only required
            params are included, when True both required and
            optional params are included """
        # uncomment below to create an instance of `IoK8sApimachineryPkgApisMetaV1Condition`
        """
        model = IoK8sApimachineryPkgApisMetaV1Condition()
        if include_optional:
            return IoK8sApimachineryPkgApisMetaV1Condition(
                last_transition_time = datetime.datetime.strptime('2013-10-20 19:20:30.00', '%Y-%m-%d %H:%M:%S.%f'),
                message = '',
                observed_generation = 56,
                reason = '',
                status = '',
                type = ''
            )
        else:
            return IoK8sApimachineryPkgApisMetaV1Condition(
                last_transition_time = datetime.datetime.strptime('2013-10-20 19:20:30.00', '%Y-%m-%d %H:%M:%S.%f'),
                message = '',
                reason = '',
                status = '',
                type = '',
        )
        """

    def testIoK8sApimachineryPkgApisMetaV1Condition(self):
        """Test IoK8sApimachineryPkgApisMetaV1Condition"""
        # inst_req_only = self.make_instance(include_optional=False)
        # inst_req_and_optional = self.make_instance(include_optional=True)

if __name__ == '__main__':
    unittest.main()
