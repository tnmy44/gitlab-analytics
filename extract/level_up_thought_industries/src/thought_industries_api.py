"""
ThoughtIndustries is the name of the vendor that provides
GitLab with Learning Management System internally known as Level Up.

The code will refer to ThoughtIndustries when referring to the API,
and Level Up when referring to the schemas/tables to save.

There is one parent class- ThoughtIndustries- and for each API endpoint,
a corresponding child class.

The parent class contains the bulk of the logic as the endpoints are very similiar.
"""
import os

from abc import ABC, abstractmethod
from typing import Any, Dict

from thought_industries_api_helpers import (
    make_request, iso8601_to_epoch_ts_ms, upload_payload_to_snowflake,
    is_invalid_ms_timestamp
)

config_dict = os.environ.copy()


class ThoughtIndustries(ABC):
    """ Base abstract class that contains the main endpoint logic """
    BASE_URL = 'https://levelup.gitlab.com/'
    HEADERS = {
        'Authorization': f'Bearer {config_dict["THOUGHT_INDUSTRIES_API_KEY"]}'
    }

    @abstractmethod
    def get_endpoint_url(self):
        """ Each child class must implement """

    @abstractmethod
    def get_name(self):
        """ Each child class must implement """

    def __init__(self):
        """ Instantiate instance vars """
        self.name = self.get_name()
        self.endpoint_url = self.get_endpoint_url()

    def fetch_from_endpoint(
        self, original_epoch_start: int, original_epoch_end: int
    ) -> Dict[Any, Any]:
        """
        Based on the start & end epoch dates, continue calling the API
        until no more data is returned.

        Note that:
        - the API returns data from latest -> earliest
        - only returns 100 records per request.

        The sliding window of start/end times looks like this:
        Start ————————— end  # first request
        Start ————— prev_min  # 2nd request
        Start —- prev_min # 3rd request
        ...
        """
        final_events = []
        events = ['_']  # some placeholder val
        current_epoch_end = original_epoch_end  # init current epoch end

        # while the response returns events records
        while len(events) > 0:
            params = {
                'startDate': original_epoch_start,
                'endDate': current_epoch_end
            }
            full_url = f'{self.BASE_URL}{self.endpoint_url}'
            print(f'\nMaking request to {full_url} with params:\n{params}')
            response = make_request(
                "GET", full_url, headers=self.HEADERS, params=params
            )

            events = response.json()['events']
            if events:
                events_to_print = [events[0]] + [events[-1]]
                print(f'\nevents_to_print: {events_to_print}')
                final_events = final_events + events

                # get the earliest event from latest response
                # it should be the last one, since api returns from latest to earliest
                current_epoch_end = iso8601_to_epoch_ts_ms(
                    events[-1]['timestamp']
                )
            else:
                print('\nThe last response had 0 events, stopping requests\n')

        return final_events

    def upload_events_to_snowflake(self, events: Dict[Any, Any]):
        """ Upload event payload to Snowflake """

        schema_name = 'level_up'
        stage_name = 'level_up_load_stage'
        table_name = self.name
        json_dump_filename = f"level_up_{self.name}.json"
        upload_payload_to_snowflake(
            events, schema_name, stage_name, table_name, json_dump_filename
        )

    def fetch_and_upload_data(self, epoch_start: int,
                              epoch_end: int) -> Dict[Any, Any]:
        """ main function, fetch data from API, and upload to snowflake """
        if is_invalid_ms_timestamp(epoch_start, epoch_end):
            raise ValueError(
                'Invalid epoch timestamp(s). Make sure epoch timestamp is in MILLISECONDS. '
                'Aborting now...'
            )
        events = self.fetch_from_endpoint(epoch_start, epoch_end)
        self.upload_events_to_snowflake(events)
        return events


class CourseCompletions(ThoughtIndustries):
    """ Class for CourseCompletions endpoint """
    def get_name(self) -> str:
        """ implement abstract class """
        return 'course_completions'

    def get_endpoint_url(self) -> str:
        """ implement abstract class """
        return "incoming/v2/events/courseCompletion"


class Logins(ThoughtIndustries):
    """ Class for Logins endpoint """
    def get_name(self) -> str:
        """ implement abstract class """
        return "logins"

    def get_endpoint_url(self) -> str:
        """ implement abstract class """
        return "incoming/v2/events/login"


class Visits(ThoughtIndustries):
    """ Class for Visits endpoint """
    def get_name(self) -> str:
        """ implement abstract class """
        return "visits"

    def get_endpoint_url(self) -> str:
        """ implement abstract class """
        return "incoming/v2/events/visit"


class CourseViews(ThoughtIndustries):
    """ Class for CourseViews endpoint """
    def get_name(self) -> str:
        """ implement abstract class """
        return "course_views"

    def get_endpoint_url(self) -> str:
        """ implement abstract class """
        return "incoming/v2/events/courseView"


if __name__ == '__main__':
    EPOCH_START = 1675904400000
    EPOCH_END = 1676246400000
    cls_to_run = CourseCompletions()
    result_events = cls_to_run.fetch_and_upload_data(EPOCH_START, EPOCH_END)
    print(f'\nresult_events: {result_events[:10]}')
