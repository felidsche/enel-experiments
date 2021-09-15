import json
import re
import sys
from os.path import exists

import pandas as pd
import requests


def get_log(path: str):
    with open(path, mode="r") as file:
        return file.read()


class ExperimentMetrics:

    def __init__(
            self, has_checkpoint: bool = False,
            hist_server_url: str = "http://localhost:18080/api/v1/"
    ):
        self.has_checkpoint = has_checkpoint
        self.hist_server_url = hist_server_url

    def get_data(self, hist_server_url: str, endpoint: str) -> dict:
        data = {}
        try:
            data = json.loads(requests.get(url=hist_server_url + endpoint).content)
        except (json.JSONDecodeError, requests.exceptions.ConnectionError) as e:
            print(f"{e} \n No values were returned, check the app_id")
        return data

    def get_app_data(self, app_id) -> pd.DataFrame:
        """
        App >> (Job) >> Stage >> Task
        For an app, get the stages and tasks of each job
        Note: The job data is skipped to get the stages and tasks
        :param app_id:
        :return:
        """
        stages_attempt_data = self.get_stages_attempt_data(app_id=app_id)
        stages_attempt_df = pd.DataFrame(stages_attempt_data).stack().apply(pd.Series).reset_index()
        try:
            stages_attempt_df = stages_attempt_df[
                ["status", "stageId", "attemptId", "numTasks", "numActiveTasks", "numCompleteTasks", "numFailedTasks",
                 "numKilledTasks", "submissionTime", "firstTaskLaunchedTime", "completionTime", "name", "rddIds",
                 "tasks"]]
            tasks_df = pd.DataFrame(stages_attempt_df["tasks"].apply(pd.Series)).apply(pd.Series).unstack(
                level=-1).apply(
                pd.Series).dropna(axis=0, how="all").reset_index(0)
            tasks_df = tasks_df[["attempt", "duration", "executorId", "index", "launchTime", "taskId"]]
            df = stages_attempt_df.join(tasks_df).drop(labels="tasks", axis=1)
            df.rename(columns={"attempt": "taskAttempt", "duration": "taskDuration", "executorId": "taskExecutorId",
                               "index": "taskIndex", "launchTime": "taskLaunchTime"}, inplace=True)
            return df
        except KeyError as e:
            print(f"{e}, No completed applications found!")
            return stages_attempt_df
        # df.to_csv(path_or_buf="/Users/fschnei4/TUB_Master_ISM/SoSe21/MA/artifacts/stage_and_task_data.csv",na_rep="nan")

    def get_stages_attempt_data(self, app_id: str) -> list:
        stages_endpoint = f"applications/{app_id}/stages/"
        stages_data = self.get_data(self.hist_server_url, endpoint=stages_endpoint)
        stages_attempts = []
        # reversed to be in chronological order
        try:
            for stage in reversed(stages_data):
                stage_id = stage["stageId"]
                stages_attempt_endpoint = f"applications/{app_id}/stages/{stage_id}"
                next_stage_attempts = self.get_data(self.hist_server_url, endpoint=stages_attempt_endpoint)
                stages_attempts.append(next_stage_attempts)
        except TypeError as e:
            print(f"{e}, No completed applications found!")
        return stages_attempts

    """
    def get_stage_data(self, app_id: str, job_data: dict) -> dict:
        for job in job_data:
            
        endpoint = f"/applications/{app_id}/stages/{stage_id}"
    """

    def get_task_list(self, hist_server_url, app_id: str, stage_id: str, stage_attempt_id: str):
        endpoint = f"applications/{app_id}/stages/{stage_id}/{stage_attempt_id}/taskList"
        task_list = self.get_data(
            hist_server_url=hist_server_url,
            endpoint=endpoint
        )

        return task_list

    def get_matches_from_log(self, log: str, pattern: str, group: int) -> list:
        """
        returns the regex matches for a given pattern from the Spark application log
        """
        re_matches = []
        matches = re.finditer(pattern=pattern, string=log)
        for match in matches:
            try:
                re_matches.append(int(match.group(group)))
            except ValueError as e:
                # match is not an int, return str
                re_matches.append(match.group(group))
        return re_matches

    def get_tcs(self, log: str) -> list:
        pattern = r"(Checkpointing took\s)(\d{2,})(\sms)"
        tcs = self.get_matches_from_log(log=log, pattern=pattern, group=2)
        return tcs

    def get_has_checkpoint(self) -> bool:
        return self.has_checkpoint

    def calc_mttr(self) -> int:
        """
        returns the mean time to recovery of a task in a job in ms
        """
        return 0

    def get_checkpoint_rdds(self, log: str) -> list:
        pattern = r"(Done\scheckpointing\sRDD\s)(\d{1,})(\sto)"
        checkpoint_rdds = self.get_matches_from_log(log=log, pattern=pattern, group=2)
        return checkpoint_rdds

    def merge_tc_rdds(self, tcs: list, rdds: list) -> dict:
        # keys: rdd_ids, values: time for checkpoint
        return dict(zip(rdds, tcs))

    def add_tc_to_app_data(self, rdd_tcs: dict, app_data: pd.DataFrame) -> pd.DataFrame:
        # contains the rddId and the corresponding tc
        rdd_tcs_df = pd.DataFrame.from_dict(rdd_tcs, orient="index", columns=["tcMs"]).reset_index()
        rdd_tcs_df.rename(columns={"index": "rddId"}, inplace=True)
        # contains only the `stageId` were at least 1 rdd was checkpointed
        rdd_tcs_unique_df = app_data.set_index("stageId").rddIds.apply(pd.Series) \
            .stack().reset_index(0, name='rddId').merge(app_data) \
            .merge(rdd_tcs_df, how='right').sort_values('rddId') \
            .reset_index(drop=True)
        # merge the records with tcMs into the initial df
        app_data_tc = app_data.merge(rdd_tcs_unique_df[["rddId", "tcMs", "stageId"]], on="stageId", how="outer")
        return app_data_tc

    def get_app_id(self, log: str) -> str:
        """
        returns the ID of a Spark App from the application log using Regex
        :param log:
        :return: s
        """
        pattern = r"(local-\d{1,})(\.inprogress)"
        app_id_matches = self.get_matches_from_log(log=log, pattern=pattern, group=1)
        app_id = app_id_matches[0]  # the first ID in the log is the actual App ID
        return app_id

    def get_app_duration(self, app_id: str) -> int:
        """

        :param app_id:
        :return: app_duration in ms (defaults to 0)
        """
        applications = self.get_data(hist_server_url=self.hist_server_url, endpoint="applications")
        default = 0
        app_duration = None
        for app in applications:
            if app["id"] != app_id:
                continue
            else:
                attempts = app["attempts"]
                for attempt in attempts:
                    app_duration = attempt["duration"]
                    return app_duration
        if app_duration is None:
            print("no duration was found for the given app_id")
            return default


if __name__ == '__main__':
    has_checkpoint = bool(int(sys.argv[1]))
    em = ExperimentMetrics(has_checkpoint=has_checkpoint)
    log_path = "/Users/fschnei4/spark-3.1.2-bin-hadoop3.2/app-logs/app.log"
    log = get_log(log_path)
    app_id = em.get_app_id(log=log)
    app_data = em.get_app_data(app_id=app_id)
    tcs = em.get_tcs(log=log)

    rdds = em.get_checkpoint_rdds(log=log)
    duration = em.get_app_duration(app_id=app_id)
    print(f"For App_ID: {app_id} The total duration was {duration} ms")
    if has_checkpoint:
        print(f"For App_ID: {app_id} The rdds: {rdds} were checkpointed and it took {sum(tcs)} ms")
    rdd_tcs = em.merge_tc_rdds(
        tcs=tcs,
        rdds=rdds
    )
    app_data_tc = em.add_tc_to_app_data(rdd_tcs=rdd_tcs, app_data=app_data)
    filepath = f"experiments_service/output/{app_id}stage_task_and_tc_data.csv"
    file_exists = exists(filepath)
    if not file_exists:
        app_data_tc.to_csv(
            path_or_buf=filepath,
            na_rep="nan"
        )

    print(f"Result:\n {app_data_tc.head(3)}")
