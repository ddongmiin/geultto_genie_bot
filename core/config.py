from core.bigquery import BigqueryProcessor


class _Config:
    SCHEMA_USERS = BigqueryProcessor.read_schema(file_path="schema/users.json")
    SCHEMA_CHANNELS = BigqueryProcessor.read_schema(file_path="schema/channels.json")
    SCHEMA_CONVERSATION = BigqueryProcessor.read_schema(file_path="schema/slack_conversation.json")


config = _Config()
