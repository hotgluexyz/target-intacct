"""
API Base class with util functions
"""
import datetime as dt
import json
import re
import sys
import uuid
from typing import Dict, List, Union
from urllib.parse import unquote

import backoff
import requests
import xmltodict

import singer

from target_intacct.exceptions import (
    ExpiredTokenError,
    InternalServerError,
    InvalidTokenError,
    NoPrivilegeError,
    NotFoundItemError,
    RetryableIntacctError,
    SageIntacctSDKError,
    WrongParamsError,
)

from .const import GET_BY_DATE_FIELD, INTACCT_OBJECTS

logger = singer.get_logger()

GATEWAY_ERROR_PATTERN = re.compile(r'^GW-\d+$')

RETRYABLE_STATUS_CODES = (429, 502, 503, 504)

MAX_RETRIES = 5


def _log_retry(details):
    _, exc, _ = sys.exc_info()
    logger.info(
        'Temporary Intacct API error: %s. Retrying in %.1f seconds (attempt %d of %d)...',
        exc,
        details['wait'],
        details['tries'],
        MAX_RETRIES,
    )


def _has_temporary_error(parsed_response: Dict) -> bool:
    """Returns True when the parsed response contains a gateway (GW-nnnn) error."""
    try:
        errors = parsed_response['response']['errormessage']['error']
    except (KeyError, TypeError):
        return False

    if isinstance(errors, dict):
        errors = [errors]

    return any(
        GATEWAY_ERROR_PATTERN.match(error.get('errorno') or '')
        for error in errors
        if isinstance(error, dict)
    )

def _format_date_for_intacct(datetime: dt.datetime) -> str:
    """
    Intacct expects datetimes in a 'MM/DD/YY HH:MM:SS' string format.
    Args:
        datetime: The datetime to be converted.

    Returns:
        'MM/DD/YY HH:MM:SS' formatted string.
    """
    return datetime.strftime('%m/%d/%Y %H:%M:%S')


class SageIntacctSDK:
    """The base class for all API classes."""

    def __init__(
        self,
        api_url: str,
        company_id: str,
        sender_id: str,
        sender_password: str,
        user_id: str,
        user_password: str,
        headers: Dict,
    ):
        self.__api_url = api_url
        self.__gateway_url = api_url
        #__gateway_url keeps the original URL; 
        # __api_url stays the mutable session endpoint. 
        # Without this split, the second login would POST credentials to the wrong host after the first auth.
        self.__company_id = company_id
        self.__sender_id = sender_id
        self.__sender_password = sender_password
        self.__user_id = user_id
        self.__user_password = user_password
        self.__headers = headers

        self._set_session_id(
            user_id=self.__user_id,
            company_id=self.__company_id,
            user_password=self.__user_password,
        )

    def _set_session_id(
        self,
        user_id: str,
        company_id: str,
        user_password: str,
        location_id: str = None,
    ):
        """
        Sets the session id for APIs.

        When location_id is set, Intacct scopes the session to that location entity
        (subsidiary). Omit it to stay at top-level.
        """

        timestamp = dt.datetime.now()
        login = {
            'userid': user_id,
            'companyid': company_id,
            'password': user_password,
        }
        if location_id:
            login['locationid'] = location_id

        dict_body = {
            'request': {
                'control': {
                    'senderid': self.__sender_id,
                    'password': self.__sender_password,
                    'controlid': timestamp,
                    'uniqueid': False,
                    'dtdversion': 3.0,
                    'includewhitespace': False,
                },
                'operation': {
                    'authentication': {
                        'login': login
                    },
                    'content': {
                        'function': {
                            '@controlid': str(uuid.uuid4()),
                            'getAPISession': None,
                        }
                    },
                },
            }
        }

        response = self._post_request(dict_body, self.__gateway_url)

        if response['authentication']['status'] == 'success':
            session_details = response['result']['data']['api']
            self.__api_url = session_details['endpoint']
            self.__session_id = session_details['sessionid']

        else:
            raise SageIntacctSDKError('Error: {0}'.format(response['errormessage']))

    def use_entity_session(self, location_id: str = None):
        """Re-authenticate, optionally scoped to a location entity."""
        self._set_session_id(
            user_id=self.__user_id,
            company_id=self.__company_id,
            user_password=self.__user_password,
            location_id=location_id,
        )

    @backoff.on_exception(
        backoff.expo,
        (RetryableIntacctError, requests.exceptions.ConnectionError),
        max_tries=MAX_RETRIES,
        factor=2,
        on_backoff=_log_retry,
    )
    @singer.utils.ratelimit(10, 1)
    def _post_request(self, dict_body: dict, api_url: str) -> Dict:
        """
        Create a HTTP post request.

        Parameters:
            dict_body (dict): HTTP POST body data for the wanted API.
            api_url (str): Url for the wanted API.

        Returns:
            A response from the request (dict).
        """

        api_headers = {'content-type': 'application/xml'}
        api_headers.update(self.__headers)
        body = xmltodict.unparse(dict_body)
        logger.info(f"Making request to {api_url} with body=[{body}]")
        response = requests.post(api_url, headers=api_headers, data=body)

        try:
            parsed_xml = xmltodict.parse(response.text)
        except Exception:
            # Gateway/proxy failures can return non-XML bodies (e.g. HTML error pages)
            if response.status_code in RETRYABLE_STATUS_CODES:
                raise RetryableIntacctError(
                    'Temporary Intacct API error (HTTP {0}): {1}'.format(
                        response.status_code, response.text
                    ),
                    response.text,
                )
            raise SageIntacctSDKError(
                'Unable to parse Intacct response (HTTP {0}): {1}'.format(
                    response.status_code, response.text
                ),
                response.text,
            )

        parsed_response = json.loads(json.dumps(parsed_xml))

        if response.status_code in RETRYABLE_STATUS_CODES or _has_temporary_error(parsed_response):
            raise RetryableIntacctError(
                'Temporary Intacct API error: {0}'.format(parsed_response), parsed_response
            )

        if response.status_code == 200:
            if parsed_response['response']['control']['status'] == 'success':
                api_response = parsed_response['response']['operation']

            if parsed_response['response']['control']['status'] == 'failure':
                exception_msg = self.decode_support_id(
                    parsed_response['response']['errormessage']
                )
                raise WrongParamsError(
                    "Some of the parameters are wrong: {0}".format(exception_msg),
                    exception_msg,
                )

            if api_response['authentication']['status'] == 'failure':
                raise InvalidTokenError(
                    "Invalid token / Incorrect credentials: {0}".format(
                        api_response["errormessage"]
                    ),
                    api_response["errormessage"],
                )

            if api_response['result']['status'] == 'success':
                return api_response

        if response.status_code == 400:
            raise WrongParamsError(
                "Some of the parameters are wrong: {0}".format(parsed_response),
                parsed_response,
            )

        if response.status_code == 401:
            raise InvalidTokenError(
                "Invalid token / Incorrect credentials: {0}".format(parsed_response), parsed_response
            )

        if response.status_code == 403:
            raise NoPrivilegeError(
                "Forbidden, the user has insufficient privilege: {0}".format(parsed_response), parsed_response
            )

        if response.status_code == 404:
            raise NotFoundItemError("Not found item with ID: {0}".format(parsed_response), parsed_response)

        if response.status_code == 498:
            raise ExpiredTokenError("Expired token, try to refresh it: {0}".format(parsed_response), parsed_response)

        if response.status_code == 500:
            raise InternalServerError("Internal server error: {0}".format(parsed_response), parsed_response)

        raise SageIntacctSDKError('Error: {0}'.format(parsed_response))

    def support_id_msg(self, errormessages) -> Union[List, Dict]:
        """
        Finds whether the error messages is list / dict and assign type and error assignment.

        Parameters:
            errormessages (dict / list): error message received from Sage Intacct.

        Returns:
            Error message assignment and type.
        """
        error = {}
        if isinstance(errormessages['error'], list):
            error['error'] = errormessages['error'][0]
            error['type'] = 'list'
        elif isinstance(errormessages['error'], dict):
            error['error'] = errormessages['error']
            error['type'] = 'dict'

        return error

    def decode_support_id(self, errormessages: Union[List, Dict]) -> Union[List, Dict]:
        """
        Decodes Support ID.

        Parameters:
            errormessages (dict / list): error message received from Sage Intacct.

        Returns:
            Same error message with decoded Support ID.
        """
        support_id_msg = self.support_id_msg(errormessages)
        data_type = support_id_msg['type']
        error = support_id_msg['error']
        if error and error['description2']:
            message = error['description2']
            support_id = re.search('Support ID: (.*)]', message)
            if support_id and support_id.group(1):
                decoded_support_id = unquote(support_id.group(1))
                message = message.replace(support_id.group(1), decoded_support_id)

        if data_type == 'list':
            errormessages['error'][0]['description2'] = message if message else None
        elif data_type == 'dict':
            errormessages['error']['description2'] = message if message else None

        return errormessages

    def format_and_send_request(self, data: Dict) -> Union[List, Dict]:
        """
        Format data accordingly to convert them to xml.

        Parameters:
            data (dict): HTTP POST body data for the wanted API.

        Returns:
            A response from the _post_request (dict).
        """

        key = next(iter(data))
        object_type = data[key]['object']

        # Remove object entry if unnecessary
        if key == "create":
            data[key].pop('object', None)

        timestamp = dt.datetime.now()

        dict_body = {
            'request': {
                'control': {
                    'senderid': self.__sender_id,
                    'password': self.__sender_password,
                    'controlid': timestamp,
                    'uniqueid': False,
                    'dtdversion': 3.0,
                    'includewhitespace': False,
                },
                'operation': {
                    'authentication': {'sessionid': self.__session_id},
                    'content': {
                        'function': {'@controlid': str(uuid.uuid4()), key: data[key]}
                    },
                },
            }
        }
        with singer.metrics.http_request_timer(endpoint=object_type):
            response = self._post_request(dict_body, self.__api_url)
        return response['result']

    def get_entity(
        self, *, object_type: str, fields: List[str]
    ) -> List[Dict]:
        """
        Get multiple objects of a single type from Sage Intacct.

        Returns:
            List of Dict in object_type schema.
        """
        intacct_object_type = INTACCT_OBJECTS[object_type]
        total_intacct_objects = []
        get_count = {
            'query': {
                'object': intacct_object_type,
                'select': {'field': 'RECORDNO'},
                'pagesize': '1',
                'options': {'showprivate': 'true'},
            }
        }

        response = self.format_and_send_request(get_count)
        count = int(response['data']['@totalcount'])
        pagesize = 1000
        offset = 0
        for _i in range(0, count, pagesize):
            data = {
                'query': {
                    'object': intacct_object_type,
                    'select': {'field': fields},
                    'options': {'showprivate': 'true'},
                    'pagesize': pagesize,
                    'offset': offset,
                }
            }
            intacct_objects = self.format_and_send_request(data)['data'][
                intacct_object_type
            ]
            # When only 1 object is found, Intacct returns a dict, otherwise it returns a list of dicts.
            if isinstance(intacct_objects, dict):
                intacct_objects = [intacct_objects]

            total_intacct_objects = total_intacct_objects + intacct_objects

            offset = offset + pagesize
        return total_intacct_objects

    def get_sample(self, intacct_object: str):
        """
        Get a sample of data from an endpoint, useful for determining schemas.
        Returns:
            List of Dict in objects schema.
        """
        data = {
            'readByQuery': {
                'object': intacct_object.upper(),
                'fields': '*',
                'query': None,
                'pagesize': '10',
            }
        }

        return self.format_and_send_request(data)['data'][intacct_object.lower()]

    def get_match(self, intacct_object: str, query: str):
        """
        Get a sample of data from an endpoint, useful for determining schemas.
        Returns:
            List of Dict in objects schema.
        """
        data = {
            'readByQuery': {
                'object': intacct_object.upper(),
                'fields': '*',
                'query': query,
                'pagesize': '10',
            }
        }

        resp = self.format_and_send_request(data)['data']

        if intacct_object.lower() not in resp:
            raise Exception(f"Invalid custom_field {intacct_object}. Queried Intacct with [{data}] and got resp=[{resp}]")

        return resp[intacct_object.lower()]

    def get_definition(self, intacct_object: str):
        """
        Get a sample of data from an endpoint, useful for determining schemas.
        Returns:
            List of Dict in objects schema.
        """
        data = {
            'lookup': {
                'object': intacct_object.upper()
            }
        }

        response = self.format_and_send_request(data)
        return response


    def get_data(self, intacct_object: str):
        """
        Get a sample of data from an endpoint, useful for determining schemas.
        Returns:
            List of Dict in objects schema.
        """
        data = {
            'read': {
                'object': intacct_object.upper(),
                'keys': 5,
                'fields': '*'
            }
        }

        response = self.format_and_send_request(data)
        return response


    def post_journal(self, journal):
        """
        Post journal to Intacct
        """
        data = {
            'create': {
                'object': 'GLBATCH',
                'GLBATCH': journal
            }
        }

        response = self.format_and_send_request(data)
        return response


    def delete_journal(self, recordno):
        data = {
            'delete': {
                'object': 'GLBATCH',
                'keys': recordno
            }
        }

        response = self.format_and_send_request(data)
        return response

def get_client(
    *,
    api_url: str,
    company_id: str,
    sender_id: str,
    sender_password: str,
    user_id: str,
    user_password: str,
    headers: Dict,
) -> SageIntacctSDK:
    """
    Initializes and returns a SageIntacctSDK object.
    """
    connection = SageIntacctSDK(
        api_url=api_url,
        company_id=company_id,
        sender_id=sender_id,
        sender_password=sender_password,
        user_id=user_id,
        user_password=user_password,
        headers=headers,
    )

    return connection
