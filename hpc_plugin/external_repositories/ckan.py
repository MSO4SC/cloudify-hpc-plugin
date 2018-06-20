from external_repository import ExternalRepository


class Ckan(ExternalRepository):

    def __init__(self, publish_item):
        super().__init__(publish_item)

        self.entrypoint = publish_item['entrypoint']
        self.api_key = publish_item['api_key']
        self.dataset = publish_item['dataset']
        self.file_path = publish_item['file_path']

    def _build_publish_call(self, logger):
        # detach call??
        operation = "create"
        # TODO: if resource file exists, operation="update"
        call = "curl -H'Authorization: " + self.api_key + "' " + \
            "'" + self.entrypoint + "/api/action/resource_" + operation + "' " + \
            "--form upload=@ " + self.file_path + \
            "--form package_id=" + self.package_id
        return call
