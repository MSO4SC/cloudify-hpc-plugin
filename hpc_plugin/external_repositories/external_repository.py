from hpc_plugin.ssh import SshClient


class ExternalRepository(object):

    def factory(publish_item):
        if publish_item['type'] == "CKAN":  # TODO: manage key error
            from ckan import Ckan
            return Ckan(publish_item)
        else:
            return None
    factory = staticmethod(factory)

    def __init__(self, publish_item):
        self.er_type = publish_item['type']  # TODO: manage key error

    def publish(self,
                ssh_client,
                logger,
                workdir=None):
        """
        Publish the local file in the external repository

        @type ssh_client: SshClient
        @param ssh_client: ssh client connected to an HPC login node
        @rtype string
        @return TODO:
        """
        if not SshClient.check_ssh_client(ssh_client, logger):
            return False

        call = self._build_publish_call(logger)
        if call is None:
            return False

        return ssh_client.execute_shell_command(
            call,
            workdir=workdir)

    def _build_publish_call(self, logger):
        """
        Creates a script to publish the local file

        @rtype string
        @return string with the publish call. None if an error arise.
        """
        raise NotImplementedError("'_build_publish_call' not implemented.")
