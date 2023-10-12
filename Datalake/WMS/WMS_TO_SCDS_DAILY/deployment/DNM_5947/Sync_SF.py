import json
from Datalake.utils.mergeUtils import mergeToSFv2


# env = env
# deltaTable = deltaTable
# primaryKeys = primaryKeys
# conditionCols =
# primaryKeys_list = json.dumps(primaryKeys)
# conditionCols_list = json.dumps(conditionCols)

spark: SparkSession = SparkSession.getActiveSession()
mergeToSFv2("qa", "test", ["id1", "id2"], ["created_at", "updated_at"])
