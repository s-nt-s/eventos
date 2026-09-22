from subprocess import check_output, CalledProcessError
from functools import cached_property
import re
from core.util import get_env
from os import getcwd


class Git:
    @cached_property
    def root(self):
        return self.__run("rev-parse", "--show-toplevel") or getcwd()

    @cached_property
    def mail(self):
        for val in (
            get_env('GITHUB_MAIL'),
            self.__run("config", "user.email")
        ):
            if val:
                return val
        val = get_env('GITHUB_ACTOR', 'GITHUB_REPOSITORY_OWNER', default='unknown')
        return f"{val}@github.com"

    @cached_property
    def remote(self):
        host = get_env('GITHUB_SERVER_URL', default='https://github.com')
        val = get_env('GITHUB_REPOSITORY')
        if val:
            return f"{host}/{val}"
        remote = self.__run("remote", "get-url", "origin")
        if remote.endswith(".git"):
            remote = remote[:-4]
        if remote.startswith("git@"):
            remote = "https://" + remote.replace("git@", "").replace(":", "/")
        return remote.rstrip("/")

    @cached_property
    def page(self):
        m = re.match(r"^https://github.com/(.*?)/(.*?)$", self.remote)
        if not m:
            return None
        user, project = m.groups()
        return f"https://{user}.github.io/{project}"

    def __run(self, *args):
        try:
            return check_output(["git"] + list(args), encoding="utf-8").strip()
        except (FileNotFoundError, CalledProcessError):
            return None

G = Git()
