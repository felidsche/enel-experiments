import re
import requests
import json


class ExperimentMetrics:

    def __init__(
            self, has_checkpoint: bool = False,
            hist_server_url: str = "http://localhost:18080/api/v1/"
    ):
        self.has_checkpoint = has_checkpoint
        self.hist_server_url = hist_server_url

    def get_data_from_hist_server(self, hist_server_url: str, endpoint: str) -> dict:
        data = {}
        try:
            data = json.loads(requests.get(url=hist_server_url + endpoint).content)
        except json.JSONDecodeError as e:
            print(f"{e} \n No values were returned")
        return data

    def get_job_data_from_hist_server(self, hist_server_url):

        pass

    def get_task_list_from_hist_server(self, hist_server_url, app_id: str, stage_id: str, stage_attempt_id: str):
        endpoint = f"applications/{app_id}/stages/{stage_id}/{stage_attempt_id}/taskList"
        task_list = self.get_data_from_hist_server(
            hist_server_url=hist_server_url,
            endpoint=endpoint
        )


        return task_list

    def get_tc(self, log: str) -> int:
        """
        returns the time taken for checkpoints of a task in a job in ms
        """
        pattern = r"(Checkpointing took\s)(\d{2,})(\sms)"
        tc = 0
        count = 0
        matches = re.finditer(pattern=pattern, string=log)
        for match in matches:
            count += 1
            tc += int(match.group(2))
        print(f"{count} checkpoints are in the log")
        return tc

    def get_has_checkpoint(self) -> bool:
        return self.has_checkpoint

    def calc_mttr(self) -> int:
        """
        returns the mean time to recovery of a task in a job in ms
        """
        return 0
