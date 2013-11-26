import os
import re
import sys
import datetime
from functools import wraps
from getpass import getpass, getuser
from glob import glob
from contextlib import contextmanager


from fabric.api import env, cd, prefix, sudo as _sudo, run as _run, hide, task
from fabric.contrib.files import exists, upload_template
from fabric.colors import yellow, green, blue, red
from fabric.context_managers import shell_env

sys.path.append('./deploy/fabric')
now = datetime.datetime.now()
################
# Config setup #
################

conf = {}
if sys.argv[0].split(os.sep)[-1] == "fab":
   # Ensure we import settings from the current dir
   try:
       conf = __import__(env.fab_settings, globals(), locals(), [], 0).FABRIC
       try:
           conf["HOSTS"][0]
       except (KeyError, ValueError):
           raise ImportError
   except (ImportError, AttributeError):
       print "Aborting, no hosts defined."
       exit()

env.db_pass = conf.get("DB_PASS", None)
env.admin_pass = conf.get("ADMIN_PASS", None)
env.user = conf.get("SSH_USER", getuser())
env.password = conf.get("SSH_PASS", None)
env.unixgrp = conf.get("SSH_USER_GRP", None)
env.key_filename = conf.get("SSH_KEY_PATH", None)
env.hosts = conf.get("HOSTS", [])
env.base = conf.get("BASE", "/opt")
env.proj_name = conf.get("PROJECT_NAME", os.getcwd().split(os.sep)[-1])
env.venv_home = conf.get("VIRTUALENV_HOME", "/home/%s" % env.user)
env.venv_path = "%s/%s" % (env.venv_home, env.proj_name)
env.proj_home = conf.get("PROJECT_HOME", "/home/%s" % env.user)
#env.proj_path = conf.get("PROJECT_PATH", None) #GR
env.proj_path = "%s/%s" % (env.proj_home, env.proj_name)#GR
env.manage = "%s/bin/python %s/manage.py" % (env.venv_path, env.proj_path)
env.live_host = conf.get("LIVE_HOSTNAME", env.hosts[0] if env.hosts else None)
env.repo_url = conf.get("REPO_URL", None)
env.reqs_path = conf.get("REQUIREMENTS_PATH", None)
env.gunicorn_port = conf.get("GUNICORN_PORT", 8000)
env.gunicorn_loglevel = conf.get("GUNICORN_LOGLEVEL", "debug")
env.locale = conf.get("LOCALE", "en_US.UTF-8")
now = datetime.datetime.now()
env.time = now.strftime("%Y%m%d-%H%M%S")
##################
# Template setup #
##################

# Each template gets uploaded at deploy time, only if their
# contents has changed, in which case, the reload command is
# also run.

templates = {
   "nginx": {
       "local_path": "deploy/nginx.conf",
       "remote_path": "%(proj_path)s/deploy/%(proj_name)s.nginx.conf",
        "move_to": "/etc/nginx/sites-enabled/%(proj_name)s.conf",
        #"reload_command": "service nginx restart",
   },
   #"supervisor": {
   #    "local_path": "deploy/supervisor.conf",
   #    #"remote_path": "/etc/supervisor/conf.d/%(proj_name)s.conf",
   #    "remote_path": "%(proj_path)s/deploy/%(proj_name)s.supervisor.conf",
   #    #"move_to": "/etc/supervisor/conf.d/%(proj_name)s.conf",
   #   #"reload_command": "supervisorctl reload",
   #},
   "upstart": {
       "local_path": "deploy/upstart.conf",
       "remote_path": "%(proj_path)s/deploy/%(proj_name)s.upstart.conf",
       "move_to": "/etc/init/%(proj_name)s.conf",
       #"reload_command": "%(proj_name)s restart",
   },
   "upstartrun": {
       "local_path": "deploy/upstart_run.conf",
       "remote_path": "%(proj_path)s/%(proj_name)s.run_via_gunicorn.sh",
       "mode": "555",
        "reload_command": "service %(proj_name)s restart",
   },
   "cron": {
       "local_path": "deploy/crontab",
       "remote_path": "%(proj_path)s/deploy/%(proj_name)s.cron.conf",
       "move_to": "/etc/cron.d/%(proj_name)s",
        "owner": "root",
       "mode": "600",
   },
   "gunicorn": {
       "local_path": "deploy/gunicorn.conf.py",
       "remote_path": "%(proj_path)s/gunicorn.conf.py",
   },
   "settings": {
       "local_path": "deploy/live_settings.py",
       "remote_path": "%(proj_path)s/local_settings.py",
   },
}


######################################
# Context for virtualenv and project #
######################################

@contextmanager
def virtualenv():
   """
    Runs commands within the project's virtualenv.
    """
   with cd(env.venv_path):
       with prefix("source %s/bin/activate" % env.venv_path):
           yield


@contextmanager
def setsetting():
    """
    Sets the export DJANGO_SETTINGS_MODULE to be something like this =ngsdb03.settings.gramasamy02
    """
    with shell_env(DJANGO_SETTINGS_MODULE="ngsdb03.settings.gramasamy02"):
        yield

@contextmanager
def project():
   """
    Runs commands within the project's directory.
    """
   with virtualenv():
       with setsetting():
            with cd(env.proj_path):
                yield


@contextmanager
def update_changed_requirements():
   """
    Checks for changes in the requirements file across an update,
    and gets new requirements if changes have occurred.
    """
   reqs_path = os.path.join(env.proj_path, env.reqs_path)
   get_reqs = lambda: run("cat %s" % reqs_path, show=False)
   old_reqs = get_reqs() if env.reqs_path else ""
   yield
   if old_reqs:
       new_reqs = get_reqs()
       if old_reqs == new_reqs:
           # Unpinned requirements should always be checked.
           for req in new_reqs.split("\n"):
               if req.startswith("-e"):
                   if "@" not in req:
                       # Editable requirement without pinned commit.
                       break
               elif req.strip() and not req.startswith("#"):
                   if not set(">=<") & set(req):
                       # PyPI requirement without version.
                       break
           else:
               # All requirements are pinned.
               return
       pip("-r %s/%s" % (env.proj_path, env.reqs_path))


###########################################
# Utils and wrappers for various commands #
###########################################

def _print(output):
   print
   print output
   print


def print_command(command):
   _print(blue("$ ", bold=True) +
          yellow(command, bold=True) +
          red(" ->", bold=True))


@task
def run(command, show=True):
   """
    Runs a shell comand on the remote server.
    """
   if show:
       print_command(command)
   with hide("running"):
       return _run(command)


@task
def sudo(command, show=True):
   """
    Runs a command as sudo.
    """
   if show:
       print_command(command)
   with hide("running"):
       return _sudo(command)


def log_call(func):
   @wraps(func)
   def logged(*args, **kawrgs):
       header = "-" * len(func.__name__)
       _print(green("\n".join([header, func.__name__, header]), bold=True))
       return func(*args, **kawrgs)
   return logged


def get_templates():
   """
    Returns each of the templates with env vars injected.
    """
   injected = {}
   for name, data in templates.items():
       injected[name] = dict([(k, v % env) for k, v in data.items()])
   return injected


def upload_template_and_reload(name):
   """
    Uploads a template only if it has changed, and if so, reload a
    related service.
    """
   template = get_templates()[name]
   local_path = template["local_path"]
   remote_path = template["remote_path"]
   move_to = template.get("move_to")
   reload_command = template.get("reload_command")
   owner = template.get("owner")
   mode = template.get("mode")
   remote_data = ""
   if name == "upstartrun":
       run("rm -rf %s" %remote_path)
   if exists(remote_path):
       with hide("stdout"):
           remote_data = sudo("cat %s" % remote_path, show=False)
   with open(local_path, "r") as f:
       local_data = f.read()
       # Escape all non-string-formatting-placeholder occurrences of '%':
       local_data = re.sub(r"%(?!\(\w+\)s)", "%%", local_data)
       if "%(db_pass)s" in local_data:
           env.db_pass = db_pass()
       local_data %= env
   clean = lambda s: s.replace("\n", "").replace("\r", "").strip()
   if clean(remote_data) == clean(local_data):
       return
   upload_template(local_path, remote_path, env, use_sudo=False, backup=False)
   if move_to:
       sudo("cp %s %s" % (remote_path, move_to))
   if owner:
       sudo("chown %s %s" % (owner, move_to))
   if name == 'upstartrun':
        if mode:
           sudo("chmod %s %s" % (mode, remote_path))
   else:
        if mode:
            sudo("chmod %s %s" % (mode, move_to))
   if reload_command:
       sudo(reload_command)

def db_pass():
   """
    Prompts for the database password if unknown.
    """
   if not env.db_pass:
       env.db_pass = getpass("Enter the database password: ")
   return env.db_pass


@task
def apt(packages):
   """
    Installs one or more system packages via apt.
    """
   return sudo("apt-get install -y -q " + packages)


@task
def pip(packages):
   """
    Installs one or more Python packages within the virtual environment.
    """
   with virtualenv():
       return sudo("pip install %s" % packages)


def postgres(command):
   """
    Runs the given command as the postgres user.
    """
   show = not command.startswith("psql")
   return run("sudo -u root sudo -u postgres %s" % command, show=show)


@task
def psql(sql, show=True):
   """
    Runs SQL against the project's database.
    """
   out = postgres('psql -c "%s"' % sql)
   if show:
       print_command(sql)
   return out


@task
def backup(filename):
   """
    Backs up the database.
    """
   return postgres("pg_dump -Fc %s > %s" % (env.proj_name, filename))


@task
def restore(filename):
   """
    Restores the database.
    """
   return postgres("pg_restore -c -d %s %s" % (env.proj_name, filename))


@task
def python(code, show=True):
   """
    Runs Python code in the project's virtual environment, with Django loaded.
    """
   setup = "import os; os.environ[\'DJANGO_SETTINGS_MODULE\']=\'settings\';"
   full_code = 'python -c "%s%s"' % (setup, code.replace("`", "\\\`"))
   with project():
       result = run(full_code, show=False)
       if show:
           print_command(code)
   return result


def static():
   """
    Returns the live STATIC_ROOT directory.
    """
   return python("from django.conf import settings;"
                 "print settings.STATIC_ROOT").split("\n")[-1]


@task
def manage(command):
   """
    Runs a Django management command.
    """
   return run("%s %s" % (env.manage, command))


@task
def filecopy(file):
   """
    Copies a file from source to destination folder
    """
   run("cp %s %s" % (file, '/tmp'))

#########################
# Install and configure #
#########################

@task
@log_call
def install():
   """
    Installs the base system and Python requirements for the entire server.
    """
   locale = "LC_ALL=%s" % env.locale
   with hide("stdout"):
       if locale not in sudo("cat /etc/default/locale"):
           sudo("update-locale %s" % locale)
           run("exit")
   #sudo("apt-get update -y -q")
   apt("g++ nginx libjpeg-dev python-dev python-setuptools libpng12-dev libgif-dev git-core "
       "postgresql libpq-dev memcached supervisor")
   if not exists("/usr/lib/libjpeg.so"):
        sudo("ln -s /usr/lib/x86_64-linux-gnu/libjpeg.so /usr/lib")
   #requirements for connecting to MS SQL
   apt("unixodbc unixodbc-dev freetds-dev tdsodbc")
   sudo("easy_install pip")
   sudo("pip install virtualenv")



@task
@log_call
def setup():
        """
        Sets up necessory directories in BASE dir (default:/opt). And adjusts the permissions to the user:grp (from root).
        """
        with cd(env.base):
            if not exists(env.venv_home):
                sudo("mkdir -p %s" % env.venv_home)
                sudo("chown %s:%s %s" % (env.user, env.unixgrp, env.venv_home))
            else:
                print "Exists %s; Nothing done!\n" % env.venv_home

            if not exists(env.proj_home):
               sudo("mkdir -p %s" % env.proj_home)
               sudo("chown %s:%s %s" % (env.user, env.unixgrp, env.proj_home))
            else:
               print "Exists %s; Nothing done!\n" % env.proj_home

        #if not exists("/opt/hello/test"):
        #sudo("mkdir -p %s" % "/opt/hello/test")
        #sudo("chown %s:%s %s" % (env.user, env.unixgrp, "/opt/hello/test"))
@task
@log_call
def pgdump():
       """
        Does a pg_dump to back up the database
        """
       with cd(env.base):
           prompt = raw_input("\nFile path to dump the %s database :" % env.proj_name)
           if not prompt:
               print"\nNo path is given\nAborting!"
               return False
           else:
               pg_dump_path=prompt
               print pg_dump_path
           run("pg_dump -U %s -h localhost %s >  %s/%s.latest_dump_from_%s.sql;" %  (env.proj_name, env.proj_name, pg_dump_path, env.proj_name, env.hosts[0]))

@task
@log_call
def rebuild_db():
        """
        Removes the existing database (with proj_name) if any; creates new database (with proj_name) and sets owner to proj_name
        Restores the data from given dump file
        """
        with cd(env.base):
            pgdump()
            print "Dumped latest database. If this needs to be restored, please use this file with rebuild_db function.\n"
            prompt = raw_input("\nDump file path to restore the %s database from:" % env.proj_name)
            if not prompt:
                print"\nNo file is given\nAborting!"
                return False
            else:
                pg_dump_file=prompt
                print pg_dump_file
            psql("DROP DATABASE %s;" % env.proj_name)
            psql("CREATE DATABASE %s WITH OWNER %s ENCODING = 'UTF8' "
               "LC_CTYPE = '%s' LC_COLLATE = '%s' TEMPLATE template0;" %
               (env.proj_name, env.proj_name, env.locale, env.locale))
            if '.sql' in pg_dump_file:
                restore_method = 'SQL'
                print "Restore Method = %s" % restore_method
                sudo("psql -U %s -d %s -h localhost -f %s" %(env.proj_name, env.proj_name, pg_dump_file))
            else:
                restore_method = 'DUMP'
                print "Restore Method = %s" % restore_method
                sudo("pg_restore -U %s -d %s -h localhost -f %s" %(env.proj_name, env.proj_name, pg_dump_file))


@task
@log_call
def create():
   """
    Create a new virtual environment for a project.
    Pulls the project's repo from version control, adds system-level
    configs for the project, and initialises the database with the
    live host.
    """

   # Create virtualenv
   with cd(env.venv_home):
       if exists(env.proj_name):
           prompt = raw_input("\nVirtualenv exists: %s\nWould you like "
                              "to replace it? (yes/no) " % env.proj_name)
           if prompt.lower() != "yes":
               print "\nAborting!"
               return False
           remove()
       run("virtualenv %s --distribute" % env.proj_name)
       #GR add env.variable for settings module DJANGO_SETTINGS_MODULE
       sudo("echo \"export DJANGO_SETTINGS_MODULE=%s.settings.%s\" >> %s/bin/activate" % (env.proj_name, env.fab_settings, env.venv_path))

       vcs = "git" if env.repo_url.endswith("git") else "hg"
       run("%s clone %s %s" % (vcs, env.repo_url, env.proj_path))

   # Create DB and DB user.
   pw = db_pass()
   user_sql_args = (env.proj_name, pw.replace("'", "\'"))
   user_sql = "CREATE USER %s WITH ENCRYPTED PASSWORD '%s';" % user_sql_args
   psql(user_sql, show=False)
   shadowed = "*" * len(pw)
   print_command(user_sql.replace("'%s'" % pw, "'%s'" % shadowed))
   psql("CREATE DATABASE %s WITH OWNER %s ENCODING = 'UTF8' "
        "LC_CTYPE = '%s' LC_COLLATE = '%s' TEMPLATE template0;" %
        (env.proj_name, env.proj_name, env.locale, env.locale))

   # Set up SSL certificate.
   conf_path = "/etc/nginx/conf"
   if not exists(conf_path):
       sudo("mkdir %s" % conf_path)
   with cd(conf_path):
       crt_file = env.proj_name + ".crt"
       key_file = env.proj_name + ".key"
       if not exists(crt_file) and not exists(key_file):
           try:
               crt_local, = glob(os.path.join("deploy", "*.crt"))
               key_local, = glob(os.path.join("deploy", "*.key"))
           except ValueError:
               parts = (crt_file, key_file, env.live_host)
               sudo("openssl req -new -x509 -nodes -out %s -keyout %s "
                    "-subj '/CN=%s' -days 3650" % parts)
           else:
               upload_template(crt_local, crt_file, use_sudo=True)
               upload_template(key_local, key_file, use_sudo=True)
   # Set up project.
   upload_template_and_reload("settings")
   with project():
       if env.reqs_path:
           pip("-r %s/%s" % (env.proj_path, env.reqs_path))
       pip("gunicorn setproctitle south psycopg2 "
           "django-compressor python-memcached")
       #manage("createdb --noinput --nodata")

       #python("from django.conf import settings;"
       #       "from django.contrib.sites.models import Site;"
       #       "site, _ = Site.objects.get_or_create(id=settings.SITE_ID);"
       #       "site.domain = '" + env.live_host + "';"
       #       "site.save();")
       if env.admin_pass:
           pw = env.admin_pass
           user_py = ("from django.contrib.auth.models import User;"
                      "u, _ = User.objects.get_or_create(username='admin');"
                      "u.is_staff = u.is_superuser = True;"
                      "u.set_password('%s');"
                      "u.save();" % pw)
           python(user_py, show=False)
           shadowed = "*" * len(pw)
           print_command(user_py.replace("'%s'" % pw, "'%s'" % shadowed))
   return True


@task
@log_call
def remove():
   """
    Blow away the current project.
    """
   if exists(env.venv_path):
       sudo("rm -rf %s" % env.venv_path)
   if exists(env.proj_path):
        sudo("rm -rf %s" % env.proj_path)
   for template in get_templates().values():
       remote_path = template["remote_path"]
       if exists(remote_path):
           sudo("rm %s" % remote_path)
   psql("DROP DATABASE %s;" % env.proj_name)
   psql("DROP USER %s;" % env.proj_name)


##############
# Deployment #
##############

@task
@log_call
def restart():
   """
    Restart gunicorn worker processes and nginx server for the project.
    """
   sudo("service %s restart" % env.proj_name)
   sudo("service nginx restart")

@task
@log_call
def stop():
   """
    Stops gunicorn worker and nginx server for the project.
    """
   sudo("service %s stop" % env.proj_name)
   sudo("service nginx stop")

@task
@log_call
def setup_nginx():
   """
    This function sets the nginx for first time. Once setup, there is no need to run this for every deployment. (unless the project names changes).
    """
   with cd("/etc/nginx/conf"):
        sudo("openssl genrsa -des3 -out %s.key 1024" % env.proj_name)
        sudo("openssl req -new -key %s.key -out %s.csr" % (env.proj_name, env.proj_name))
        sudo("cp %s.key %s.key.org" % (env.proj_name, env.proj_name))
        sudo("openssl rsa -in %s.key.org -out %s.key" % (env.proj_name, env.proj_name))
        sudo("openssl x509 -req -days 365 -in %s.csr -signkey %s.key -out %s.crt" % (env.proj_name, env.proj_name, env.proj_name))

@task
@log_call
def start():
   """
    Starts gunicorn worker and nginx server for the project.
    """
   sudo("service %s start" % env.proj_name)
   sudo("service nginx start")

@task
@log_call
def deploy_code():
   """
    This functions just checks out the code from git repo and restarts the server.
    Does not touch the database
    """
   # Create virtualenv
   with cd(env.base):
           if not exists(env.proj_path):
                print "Project dir %s does not exists!. Is yhis a new deployment?\n" % env.proj_path
                print "Exists %s; Nothing done!\n" % env.proj_path
           else:
                with cd(env.proj_home):
                   run("tar -cvf %s_%s.tar %s" % (env.time, env.proj_name, env.proj_name))
                   run("gzip %s_%s.tar" % (env.time, env.proj_name))
                with cd(env.proj_path):
                   run("git pull origin master")
                   restart()


@task
@log_call
def deploy():
   """
    Deploy latest version of the project.
    Check out the latest version of the project from version
    control, install new requirements, sync and migrate the database,
    collect any new static assets, and restart gunicorn's work
    processes for the project.
    """
   if not exists(env.venv_path):
       prompt = raw_input("\nVirtualenv doesn't exist: %s\nWould you like "
                          "to create it? (yes/no) " % env.proj_name)
       if prompt.lower() != "yes":
           print "\nAborting!"
           return False
       create()
   for name in get_templates():
        print name
        upload_template_and_reload(name)
   with project():
       backup("last.db")
        #GR run("tar -cf last.tar %s" % static())
       git = env.repo_url.endswith("git")
       run("%s > last.commit" % "git rev-parse HEAD" if git else "hg id -i")
       with update_changed_requirements():
            run("git pull origin master" if git else "hg pull && hg up -C")
            run("git stash")
       print "ran git02"
       manage("collectstatic -v 0 --noinput")
       print "mangedcollectstatic"
       manage("syncdb --noinput")
       print "synced"
       manage("migrate --noinput")
       print "mirated"
   restart()
   return True


@task
@log_call
def rollback():
   """
    Reverts project state to the last deploy.
    When a deploy is performed, the current state of the project is
    backed up. This includes the last commit checked out, the database,
    and all static files. Calling rollback will revert all of these to
    their state prior to the last deploy.
    """
   with project():
       with update_changed_requirements():
           git = env.repo_url.startswith("git")
           update = "git checkout" if git else "hg up -C"
           run("%s `cat last.commit`" % update)
       with cd(os.path.join(static(), "..")):
           run("tar -xf %s" % os.path.join(env.proj_path, "last.tar"))
       restore("last.db")
   restart()


@task
@log_call
def all():
   """
    Installs everything required on a new system and deploy.
    From the base software, up to the deployed project.
    """
   install()
   setup()
   if create():
       deploy()
