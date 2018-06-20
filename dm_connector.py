import json
import time
import requests
import urllib2

from constant_info import PACKAGE_LIST, PACKAGE_LINK, APPLICATION_PAYLOAD, APPLICATION_NAME
import utils

#contants
LOGGER = None

class DeploymentManager(object):
    def __init__(self, dm_host, dm_port, pkgm_port, logger):
        self.dm_host = dm_host
        self.dm_port = dm_port
        self.pkgm_port = pkgm_port

        global LOGGER
        LOGGER = logger

    def download_upload_package(self, test_num):
        pkg_name = eval("PACKAGE_LIST.TEST%d" % test_num)
        download_command = "wget -O /tmp/%s.tar.gz %s" % (pkg_name, eval("PACKAGE_LINK.TEST%d" % test_num))
        upload_command = "curl -X PUT %s:%d/packages/%s.tar.gz?user.name=pnda --upload-file /tmp/%s.tar.gz" % (self.dm_host, self.pkgm_port, pkg_name, pkg_name)
        commands = [download_command, upload_command]
        is_download_success = utils.exe_cli(commands)
        return is_download_success

    def deploy_package(self, test_num):
        is_deploy_success = False
        pkg_name = eval("PACKAGE_LIST.TEST%d" % test_num)
        uri = "http://%s:%d/packages/%s?user.name=pnda" % (self.dm_host, self.dm_port, pkg_name)

        res = requests.put(uri)
        print res.text
        if res.status_code == 202:
            LOGGER.info("Deployed %s", pkg_name)
            is_deploy_success = True
        else:
            LOGGER.error("Failed to deploy %s", pkg_name)

        return is_deploy_success

    def create_application(self, test_num):
        is_application_created = False
        pkg_name = eval("PACKAGE_LIST.TEST%d" % test_num)

        pkg_check_uri = "http://%s:%d/packages/%s" % (self.dm_host, self.dm_port, pkg_name)
        while(1):
            res = requests.get(pkg_check_uri)
            if json.loads(res.text)["status"] == "DEPLOYED":
                break
            time.sleep(1)

        payload_pkg_name = '_'.join(''.join(pkg_name.split('.')[:-2]).split('-')[:-1])
        payload = eval("APPLICATION_PAYLOAD.%s" %  payload_pkg_name)
        application = eval("APPLICATION_NAME.TEST%d" % test_num)
        headers = {"content-type": "application/json"}
        uri = "http://%s:%d/applications/%s?user.name=pnda" % (self.dm_host, self.dm_port, application)

        res = requests.put(uri, data=json.dumps(payload), headers=headers)
        print res, res.text
        if res.status_code == 202:
            LOGGER.info("Created %s", application)
            is_application_created = True
        else:
            LOGGER.error("Failed to create %s", application)

        return is_application_created

