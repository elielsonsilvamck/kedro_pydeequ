from pathlib import Path
from typing import Any, Dict, Union

from kedro.config import ConfigLoader
from kedro.framework.context import KedroContext
from kedro.framework.hooks.manager import PluginManager
from pyspark import SparkConf
from pyspark.sql import SparkSession


class SparkClusterContext(KedroContext):
    def __init__(
        self,
        package_name: str,
        project_path: Union[Path, str],
        config_loader: ConfigLoader,
        hook_manager: PluginManager,
        env: str = None,
        extra_params: Dict[str, Any] = None,
    ):
        super().__init__(
            package_name, project_path, config_loader, hook_manager, env, extra_params
        )
        self.init_spark_session()

    def init_spark_session(self) -> None:
        """Initialises a SparkSession using the config defined in project's conf folder."""

        # Load the spark configuration in spark.yaml using the config loader
        parameters = self._config_loader.get("spark*", "spark*/**")
        spark_conf = SparkConf().setAll(parameters.items())

        # Initialise the spark session
        spark_session_conf = (
            SparkSession.builder.appName(self._package_name)
            .enableHiveSupport()
            .config(conf=spark_conf)
        )
        self._spark_session = spark_session_conf.getOrCreate()
        self._spark_session.sparkContext.setLogLevel("WARN")

    def get_spark_session(self) -> SparkSession:
        return self._spark_session
