# coding: utf-8

"""
    JobSet SDK

    Python SDK for the JobSet API  # noqa: E501

    The version of the OpenAPI document: v0.1.4
    Generated by: https://openapi-generator.tech
"""


import pprint
import re  # noqa: F401

import six

from jobset.configuration import Configuration


class JobsetV1alpha2FailurePolicyRule(object):
    """NOTE: This class is auto generated by OpenAPI Generator.
    Ref: https://openapi-generator.tech

    Do not edit the class manually.
    """

    """
    Attributes:
      openapi_types (dict): The key is attribute name
                            and the value is attribute type.
      attribute_map (dict): The key is attribute name
                            and the value is json key in definition.
    """
    openapi_types = {
        'action': 'str',
        'name': 'str',
        'on_job_failure_reasons': 'list[str]',
        'target_replicated_jobs': 'list[str]'
    }

    attribute_map = {
        'action': 'action',
        'name': 'name',
        'on_job_failure_reasons': 'onJobFailureReasons',
        'target_replicated_jobs': 'targetReplicatedJobs'
    }

    def __init__(self, action='', name='', on_job_failure_reasons=None, target_replicated_jobs=None, local_vars_configuration=None):  # noqa: E501
        """JobsetV1alpha2FailurePolicyRule - a model defined in OpenAPI"""  # noqa: E501
        if local_vars_configuration is None:
            local_vars_configuration = Configuration()
        self.local_vars_configuration = local_vars_configuration

        self._action = None
        self._name = None
        self._on_job_failure_reasons = None
        self._target_replicated_jobs = None
        self.discriminator = None

        self.action = action
        self.name = name
        self.on_job_failure_reasons = on_job_failure_reasons
        if target_replicated_jobs is not None:
            self.target_replicated_jobs = target_replicated_jobs

    @property
    def action(self):
        """Gets the action of this JobsetV1alpha2FailurePolicyRule.  # noqa: E501

        The action to take if the rule is matched.  # noqa: E501

        :return: The action of this JobsetV1alpha2FailurePolicyRule.  # noqa: E501
        :rtype: str
        """
        return self._action

    @action.setter
    def action(self, action):
        """Sets the action of this JobsetV1alpha2FailurePolicyRule.

        The action to take if the rule is matched.  # noqa: E501

        :param action: The action of this JobsetV1alpha2FailurePolicyRule.  # noqa: E501
        :type: str
        """
        if self.local_vars_configuration.client_side_validation and action is None:  # noqa: E501
            raise ValueError("Invalid value for `action`, must not be `None`")  # noqa: E501

        self._action = action

    @property
    def name(self):
        """Gets the name of this JobsetV1alpha2FailurePolicyRule.  # noqa: E501

        The name of the failure policy rule. The name is defaulted to 'failurePolicyRuleN' where N is the index of the failure policy rule. The name must match the regular expression \"^[A-Za-z]([A-Za-z0-9_,:]*[A-Za-z0-9_])?$\".  # noqa: E501

        :return: The name of this JobsetV1alpha2FailurePolicyRule.  # noqa: E501
        :rtype: str
        """
        return self._name

    @name.setter
    def name(self, name):
        """Sets the name of this JobsetV1alpha2FailurePolicyRule.

        The name of the failure policy rule. The name is defaulted to 'failurePolicyRuleN' where N is the index of the failure policy rule. The name must match the regular expression \"^[A-Za-z]([A-Za-z0-9_,:]*[A-Za-z0-9_])?$\".  # noqa: E501

        :param name: The name of this JobsetV1alpha2FailurePolicyRule.  # noqa: E501
        :type: str
        """
        if self.local_vars_configuration.client_side_validation and name is None:  # noqa: E501
            raise ValueError("Invalid value for `name`, must not be `None`")  # noqa: E501

        self._name = name

    @property
    def on_job_failure_reasons(self):
        """Gets the on_job_failure_reasons of this JobsetV1alpha2FailurePolicyRule.  # noqa: E501

        The requirement on the job failure reasons. The requirement is satisfied if at least one reason matches the list. The rules are evaluated in order, and the first matching rule is executed. An empty list applies the rule to any job failure reason.  # noqa: E501

        :return: The on_job_failure_reasons of this JobsetV1alpha2FailurePolicyRule.  # noqa: E501
        :rtype: list[str]
        """
        return self._on_job_failure_reasons

    @on_job_failure_reasons.setter
    def on_job_failure_reasons(self, on_job_failure_reasons):
        """Sets the on_job_failure_reasons of this JobsetV1alpha2FailurePolicyRule.

        The requirement on the job failure reasons. The requirement is satisfied if at least one reason matches the list. The rules are evaluated in order, and the first matching rule is executed. An empty list applies the rule to any job failure reason.  # noqa: E501

        :param on_job_failure_reasons: The on_job_failure_reasons of this JobsetV1alpha2FailurePolicyRule.  # noqa: E501
        :type: list[str]
        """
        if self.local_vars_configuration.client_side_validation and on_job_failure_reasons is None:  # noqa: E501
            raise ValueError("Invalid value for `on_job_failure_reasons`, must not be `None`")  # noqa: E501

        self._on_job_failure_reasons = on_job_failure_reasons

    @property
    def target_replicated_jobs(self):
        """Gets the target_replicated_jobs of this JobsetV1alpha2FailurePolicyRule.  # noqa: E501

        TargetReplicatedJobs are the names of the replicated jobs the operator applies to. An empty list will apply to all replicatedJobs.  # noqa: E501

        :return: The target_replicated_jobs of this JobsetV1alpha2FailurePolicyRule.  # noqa: E501
        :rtype: list[str]
        """
        return self._target_replicated_jobs

    @target_replicated_jobs.setter
    def target_replicated_jobs(self, target_replicated_jobs):
        """Sets the target_replicated_jobs of this JobsetV1alpha2FailurePolicyRule.

        TargetReplicatedJobs are the names of the replicated jobs the operator applies to. An empty list will apply to all replicatedJobs.  # noqa: E501

        :param target_replicated_jobs: The target_replicated_jobs of this JobsetV1alpha2FailurePolicyRule.  # noqa: E501
        :type: list[str]
        """

        self._target_replicated_jobs = target_replicated_jobs

    def to_dict(self):
        """Returns the model properties as a dict"""
        result = {}

        for attr, _ in six.iteritems(self.openapi_types):
            value = getattr(self, attr)
            if isinstance(value, list):
                result[attr] = list(map(
                    lambda x: x.to_dict() if hasattr(x, "to_dict") else x,
                    value
                ))
            elif hasattr(value, "to_dict"):
                result[attr] = value.to_dict()
            elif isinstance(value, dict):
                result[attr] = dict(map(
                    lambda item: (item[0], item[1].to_dict())
                    if hasattr(item[1], "to_dict") else item,
                    value.items()
                ))
            else:
                result[attr] = value

        return result

    def to_str(self):
        """Returns the string representation of the model"""
        return pprint.pformat(self.to_dict())

    def __repr__(self):
        """For `print` and `pprint`"""
        return self.to_str()

    def __eq__(self, other):
        """Returns true if both objects are equal"""
        if not isinstance(other, JobsetV1alpha2FailurePolicyRule):
            return False

        return self.to_dict() == other.to_dict()

    def __ne__(self, other):
        """Returns true if both objects are not equal"""
        if not isinstance(other, JobsetV1alpha2FailurePolicyRule):
            return True

        return self.to_dict() != other.to_dict()