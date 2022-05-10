from typing import Tuple
from mlflow.entities import Experiment, Run
import mlflow

# # 設定


class BackTest:
    """_summary_
    """

    def __init__(self, rule_name: str, version: str, experiment_name: str = "MLBot") -> None:
        """_summary_

        Args:
            experiment_name (str): mlflowの実験名。MLBotがデフォルト
            rule_name (str): 検証するルールの名称
            version (str): ルールのバージョン
        """
        self.experiment_name = experiment_name
        self.rule_name = rule_name
        self.version = version

    def start_mlflow(self) -> Tuple[Experiment, Run]:
        """MLFlowを開始する

        Returns:
            Tuple[Experiment, Run]
        """
        mlflow.set_experiment(self.experiment_name)
        experiment = mlflow.get_experiment_by_name(self.experiment_name)

        mlflow.start_run(experiment_id=experiment.experiment_id)
        run = mlflow.active_run()

        mlflow.set_tags(
            {
                "rule_name": self.rule_name,
                "version": self.version,
            }
        )

        return experiment, run

    def finish(self):
        mlflow.end_run()


# mlflow.log_figure(fig, "figure.png")
# mlflow.log_param("k", list(range(2, 50)))
# mlflow.log_metric("Silhouette Score", score, step=i)
