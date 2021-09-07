import re


class ExperimentMetrics:

    def __init__(self, has_checkpoint: bool = False):
        self.has_checkpoint = has_checkpoint

    def get_tc(self, log: str) -> int:
        """
        returns the time taken for checkpoints of a task in a job in ms
        """
        pattern = r"(Checkpointing took\s)(\d{2,})"
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
