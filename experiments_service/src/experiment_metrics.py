import json
import re
import pandas as pd
import requests
import logging
from os.path import exists

logger = logging.getLogger(__name__ + "ExperimentMetrics")  # holds the name of the module


class ExperimentMetrics:

    def __init__(
            self, has_checkpoint: bool = False,
            hist_server_url: str = "http://localhost:18080/api/v1/",
            local: bool = True
    ):
        self.has_checkpoint = has_checkpoint
        self.hist_server_url = hist_server_url
        self.local = local

    def get_data(self, hist_server_url: str, endpoint: str) -> dict:
        data = {}
        try:
            data = json.loads(requests.get(url=hist_server_url + endpoint).content)
        except (json.JSONDecodeError, requests.exceptions.ConnectionError) as e:
            logger.warning(f"{e}  No values were returned, check the app_id")
        return data

    def get_app_data(self, app_id, cache_df_file_path: str = None) -> pd.DataFrame:
        """
        App >> (Job) >> Stage >> Task
        For an app, get the stages and tasks of each job
        Note: The job data is skipped to get the stages and tasks
        :param cache_df_file_path:
        :param app_id:
        :return: pd.DataFrame with stageId as index
        """
        if cache_df_file_path is not None and not exists(cache_df_file_path):
            logger.info(f"Requesting the stage attempt data for app_id: {app_id} from: {self.hist_server_url}")
            stages_attempt_data = self.get_stages_attempt_data(app_id=app_id)
            stages_attempt_df = pd.DataFrame(stages_attempt_data).stack().apply(pd.Series).reset_index()
            logger.info(f"Caching stage attempt data to: {cache_df_file_path}")
            stages_attempt_df.to_pickle(cache_df_file_path)
        elif cache_df_file_path is not None and exists(cache_df_file_path):
            logger.info(f"The stage attempt data is loaded from: {cache_df_file_path}")
            stages_attempt_df = pd.read_pickle(cache_df_file_path)
        else:
            logger.info(f"Requesting the stage attempt data for app_id: {app_id} from: {self.hist_server_url}")
            stages_attempt_data = self.get_stages_attempt_data(app_id=app_id)
            stages_attempt_df = pd.DataFrame(stages_attempt_data).stack().apply(pd.Series).reset_index()

        try:
            stages_attempt_df = stages_attempt_df[
                ["status", "stageId", "attemptId", "numTasks", "numActiveTasks", "numCompleteTasks", "numFailedTasks",
                 "numKilledTasks", "submissionTime", "firstTaskLaunchedTime", "completionTime", "name", "rddIds",
                 "tasks"]]

            # get the data on task granularity
            logger.info(f"Getting the data for app_id: {app_id} on task granularity...")
            """
                1. pd.DataFrame(stages_attempt_df["tasks"].apply(pd.Series): create one column for each task in ["tasks"]
                2. unstack(): create one column per stage attempt (Type: dict) with one row for each task
                3. apply(pd.Series): unpack the dict column in multiple columns
                4. reset_index(0): remove the MultiIndex stage_attempt/task_id and go back to the default index
            """
            tasks_df = pd.DataFrame(stages_attempt_df["tasks"].apply(pd.Series))\
                .unstack()\
                .apply(pd.Series)\
                .reset_index(0)
            # select only relevant columns
            tasks_df = tasks_df[["attempt", "duration", "executorId", "index", "launchTime", "taskId"]]
            df = stages_attempt_df.join(tasks_df).drop(labels="tasks", axis=1)
            df.rename(columns={"attempt": "taskAttempt", "duration": "taskDuration", "executorId": "taskExecutorId",
                               "index": "taskIndex", "launchTime": "taskLaunchTime"}, inplace=True)
            return df
        except KeyError as e:
            logger.warning(f"{e}, No completed applications found for app_id: {app_id}!")
            return stages_attempt_df

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
            logger.warning(f"{e}, No completed applications found for app_id: {app_id}!")
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

    def task_has_checkpoint(self, rddIds: pd.Series, checkpoint_rdds):
        """
        pandas UDF to check if a task handled an RDD which was checkpointed
        :param rddIds: the row (pd.Series) with a list of RDDs which were handled in a task
        :param checkpoint_rdds: the list of RDDs which were actually checkpointed
        :return: the rddId which was checkpointed
        """
        rdd = None
        for rddId in rddIds:
            if rddId in checkpoint_rdds:
                rdd = rddId
        return rdd

    def add_tc_to_app_data(self, rdd_tcs: dict, app_data: pd.DataFrame) -> pd.DataFrame:
        # convert rdd_tcs dict to pandas dataframe
        rdd_tcs_df = pd.DataFrame.from_dict(rdd_tcs, orient="index", columns=["tcMs"]).reset_index()
        rdd_tcs_df.rename(columns={"index": "rddId"}, inplace=True)
        checkpoint_rdds = rdd_tcs.keys()
        # add a column for rows with the ID where the checkpointed RDDs is in the column "rddIds" of app_data
        app_data['rddId'] = app_data.rddIds.apply(
            lambda rddIds: self.task_has_checkpoint(rddIds=rddIds, checkpoint_rdds=checkpoint_rdds)
        )
        # add the tcms by joining
        try:
            app_data_tc = pd.merge(app_data, rdd_tcs_df, on="rddId", how="left")
            return app_data_tc
        except ValueError as e:
            logger.error(f"Error: {e}, No tcms cound be added, returning it without it")
            return app_data


    def get_app_id(self, log: str) -> str:
        """
        returns the ID of a Spark App from the application log using Regex
        :param log:
        :return: s
        """
        if self.local:

            pattern = r"(local-\d{1,})(\.inprogress)"
            group = 1
        else:
            pattern = r"(\/job-event-log\/)(spark\-[\w\d]{1,})(\.inprogress)"
            group = 2
        app_id_matches = self.get_matches_from_log(log=log, pattern=pattern, group=group)
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
            logger.warning(f"no duration was found for the given app_id: {app_id}")
            return default
