# -*- coding: utf-8 -*-
import logging

import pyspark.sql.functions as F
from pydeequ.verification import Check, VerificationResult, VerificationSuite
from pydeequ.checks import CheckLevel
from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession

logger = logging.getLogger(__name__)


def quality_sample(sample: DataFrame) -> DataFrame:
    """Preprocess the data for companies.

        Args:
            sample: Source data.
        Returns:
            Quality report data.

    """

    spark = SparkSession.getActiveSession()

    sample.show()
    check = (Check(spark, CheckLevel.Error, __name__).isComplete('Index')
             .isComplete('Living Space (sq ft)')
             .isComplete('Beds')
             .isComplete('Baths')
             .isNonNegative('Baths')
             .isComplete('Year')
             .satisfies("Year > (year(current_date()) - 10)",
                       constraintName="Built in the last 10 years",
                       assertion= lambda x: x == 1)
             .isComplete("List Price ($)")
             .isUnique("Index")
             )
    analysis_result = VerificationSuite(spark).onData(sample).addCheck(check).run()

    _result = VerificationResult.successMetricsAsDataFrame(spark, analysis_result)
    _result = _result.withColumn("table", F.lit("prm_sample"))
    _result.show(100, truncate=False)
    return _result


