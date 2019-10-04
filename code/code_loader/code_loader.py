import sys, os, io
import math
import json
import yaml
import time
import psutil
from glob import glob
from subprocess import Popen, PIPE, check_output
from code_loader.printer import CodeLoaderPrinter
from code_loader.rest_api import CodeLoaderRESTAPI
from dt_class_utils import DTProcess
from dt_avahi_utils import disable_service

# constants
LOADER_DATA_DIR = "/data/loader"
BOOT_LOG_FILE = "/data/boot-log.txt"
CPU_TEMPERATURE_FILE = "/sys/class/thermal/thermal_zone0/temp"
RECHECK_PERIOD_ON_ERROR_SEC = 10
RECHECK_PERIOD_SEC = 60
ENABLE_PRINTER = False
ENABLE_REST_API = True

STATUS_TEMPLATE = """
Current Status:

  Images to load (uncompressed):
    {images_load_tar}

  Images to load (compressed):
    {images_load_tar_gz}

  Stacks to load:
    {stacks_load}

  Stacks to run:
    {stacks_run}

  Configuration:
    - Exclude run: {exclude_run}
    - Delete: {do_delete}
"""


class CodeLoader(DTProcess):

    def __init__(self):
        DTProcess.__init__(self)
        self.images_to_load_dir = os.path.join(LOADER_DATA_DIR, 'images_to_load')
        self.stacks_to_load_dir = os.path.join(LOADER_DATA_DIR, 'stacks_to_load')
        self.stacks_to_run_dir = os.path.join(LOADER_DATA_DIR, 'stacks_to_run')
        self.images_to_load_tar = []
        self.images_to_load_tar_gz = []
        self.stacks_to_load_yaml = []
        self.stacks_to_run_yaml = []
        self.max_level = 4
        self.total = [1] * self.max_level
        self.tick = [0] * self.max_level
        self.action = [None] * self.max_level
        self.output = [None] * self.max_level
        self.busy = True
        self.error = False
        self.printer = CodeLoaderPrinter(self)
        self.rest_api = CodeLoaderRESTAPI(self)
        self.exclude_run = \
            os.environ['EXCLUDE_RUN'].lower().split(',') if 'EXCLUDE_RUN' in os.environ \
            else []
        self.do_delete = not ('NO_DELETE' in os.environ and os.environ['NO_DELETE'] == '1')

    def is_busy(self):
        return self.busy

    def start(self):
        # start status readers
        if ENABLE_PRINTER:
            self.printer.start()
            self.register_shutdown_callback(self.printer.stop)
        if ENABLE_REST_API:
            self.rest_api.start()
            self.register_shutdown_callback(self.rest_api.stop)
        # load
        recheck_period = RECHECK_PERIOD_SEC
        while not self.is_shutdown:
            try:
                self._load_configuration()
                self._run()
                disable_service('dt.device-init')
            except:
                e = '\n'.join(sys.exc_info())
                for lvl in range(self.max_level):
                    self._set_action(lvl, 'ERROR: %s' % e)
                    self.error = True
                recheck_period = RECHECK_PERIOD_ON_ERROR_SEC
            finally:
                self.printer.stop()
                time.sleep(recheck_period)
                if ENABLE_PRINTER:
                    self.printer.start()
        # stop status readers
        self.printer.stop()
        self.rest_api.stop()

    def get_status(self):
        # get disk info
        disk = psutil.disk_usage(LOADER_DATA_DIR)
        # get temperature info
        cpu_temp = cpu_temperature()
        # get progress info
        progress = self._get_progress()
        return {
            'status': 'error' if self.error else ('busy' if self.is_busy() else 'ready'),
            'progress': {
                lvl: {
                    'progress': progress[lvl],
                    'steps': {
                        'current': self.tick[lvl],
                        'total': self.total[lvl]
                    },
                    'action': self.action[lvl],
                    'output': self.output[lvl]
                } for lvl in range(self.max_level)
            },
            'disk': {
                'total': disk.total,
                'free': disk.free,
                'usage': int(disk.percent)
            },
            'cpu': {
                'usage': int(psutil.cpu_percent()),
                'temperature': cpu_temp
            }
        }

    def _load_configuration(self):
        # get full paths to files of interest
        self.images_to_load_tar = glob(os.path.join(self.images_to_load_dir, '*.tar'))
        self.images_to_load_tar_gz = glob(os.path.join(self.images_to_load_dir, '*.tar.gz'))
        self.stacks_to_load_yaml = glob(os.path.join(self.stacks_to_load_dir, '*.yaml')) + \
            glob(os.path.join(self.stacks_to_load_dir, '*.yml'))
        self.stacks_to_run_yaml = glob(os.path.join(self.stacks_to_run_dir, '*.yaml')) + \
            glob(os.path.join(self.stacks_to_run_dir, '*.yml'))
        # print current status
        print(STATUS_TEMPLATE.format(**{
            'images_load_tar' : list_files(basenames(self.images_to_load_tar)),
            'images_load_tar_gz' : list_files(basenames(self.images_to_load_tar_gz)),
            'stacks_load' : list_files(basenames(self.stacks_to_load_yaml)),
            'stacks_run' : list_files(basenames(self.stacks_to_run_yaml)),
            'exclude_run' : self.exclude_run,
            'do_delete' : self.do_delete
        }))

    def _run(self):
        self.busy = True
        # read images from stacks to load
        stacks_to_load = dict()
        for stack in self.stacks_to_load_yaml:
            stacks_to_load[stack] = self._images_in_stack(stack)
        num_images_stacks_to_load = sum(map(len, stacks_to_load.values()))
        # read images from stacks to run
        stacks_to_run = dict()
        for stack in self.stacks_to_run_yaml:
            stacks_to_run[stack] = self._images_in_stack(stack)
        num_images_stacks_to_run = sum(map(len, stacks_to_run.values()))
        # compute total number of images
        num_images = \
            len(self.images_to_load_tar) + \
            len(self.images_to_load_tar_gz) + \
            num_images_stacks_to_load + \
            num_images_stacks_to_run
        self._set_total(0, num_images)
        self._set_status(0, 'Loading Docker images')
        #
        # LOAD IMAGES =>
        # load images (uncompressed)
        self._set_total(1, len(self.images_to_load_tar))
        self._set_status(1, 'Loading uncrompressed images (.tar)')
        for archive in self.images_to_load_tar:
            self._docker_load_archive(archive)
            if self.do_delete:
                remove_file(archive)
            self._tick(1)
            self._tick(0)
            self._boot_log('loading', "Archive loaded: {}".format(os.path.basename(archive)))

        # load images (compressed)
        self._set_total(1, len(self.images_to_load_tar_gz))
        self._set_status(1, 'Loading crompressed images (.tar.gz)')
        for archive in self.images_to_load_tar_gz:
            self._docker_load_archive(archive)
            if self.do_delete:
                remove_file(archive)
            self._tick(1)
            self._tick(0)
            self._boot_log('loading', "Archive loaded: {}".format(os.path.basename(archive)))

        # load stacks (to run)
        self._set_total(1, len(self.stacks_to_run_yaml))
        self._set_status(1, 'Loading stacks we run at boot')
        for stack in self.stacks_to_run_yaml:
            stack_name = os.path.basename(stack).replace('.yaml', '').replace('.yml', '')
            self._set_total(2, 1+len(stacks_to_run[stack]))
            self._set_status(2, 'Loading stack: %s' % stack_name)
            for image in stacks_to_run[stack]:
                if not self._docker_image_exists(image):
                    self._docker_pull_image(image)
                    self._boot_log('loading', "Image loaded: {}".format(image))
                self._tick(2)
                self._tick(0)
            if stack_name.lower() not in self.exclude_run:
                self._docker_run_stack(stack, level=3)
                self._boot_log('loading', "Stack run: {}".format(stack_name))
            self._boot_log('loading', "Stack completed: {}".format(stack_name))
            self._tick(1)

        # load stacks (to load)
        self._set_total(1, len(self.stacks_to_load_yaml))
        self._set_status(1, 'Loading other stacks')
        for stack in self.stacks_to_load_yaml:
            stack_name = os.path.basename(stack).replace('.yaml', '').replace('.yml', '')
            self._set_total(2, len(stacks_to_load[stack]))
            self._set_status(2, 'Loading stack: %s' % stack_name)
            for image in stacks_to_load[stack]:
                self._docker_pull_image(image)
                self._tick(2)
                self._tick(0)
                self._boot_log('loading', "Image loaded: {}".format(image))
            if self.do_delete:
                remove_file(stack)
            self._boot_log('loading', "Stack completed: {}".format(stack_name))
            self._tick(1)
        self._boot_log('done', "All stacks up")

        # <= LOAD IMAGES
        self.busy = False

    def _boot_log(self, phase, message):
        try:
            with open(BOOT_LOG_FILE, "at") as fout:
                fout.write(json.dumps({'phase': phase, 'msg': message})+'\n')
                fout.flush()
        except:
            pass

    def _set_status(self, level, action, tick=0):
        self._set_tick(level, tick)
        self._set_action(level, action)
        self.output[level] = None

    def _tick(self, level):
        self._set_tick(level, self.tick[level]+1)

    def _set_total(self, level, total):
        self.total[level] = total

    def _set_tick(self, level, tick):
        self.tick[level] = tick

    def _set_action(self, action_lvl, action):
        for lvl in range(action_lvl+1, self.max_level, 1):
            self.action[lvl] = None
            self.tick[lvl] = 0
        self.action[action_lvl] = action

    def _get_progress(self):
        progress = [0] * self.max_level
        for lvl in range(self.max_level-1, -1, -1):
            progress_base = percentage(self.tick[lvl], self.total[lvl])
            substep_progress = 0
            if (lvl+1) < self.max_level and self.total[lvl] > 0:
                substep_progress = progress[lvl+1]
                substep_progress = int(math.floor(substep_progress * (1.0 / float(self.total[lvl]))))
            progress[lvl] = progress_base + substep_progress
        return progress

    def _docker_load_archive(self, archive_file, buffer_mb=8, level=2):
        self._set_status(level, 'Loading archive: %s' % os.path.basename(archive_file))
        total_bytes = os.stat(archive_file).st_size
        self._set_total(level, total_bytes)
        bytes_transferred = 0
        buffer = buffer_mb * 1024 * 1024
        docker_load_process = Popen(['docker', 'load'], stdin=PIPE, stdout=PIPE)
        with open(archive_file, 'rb') as fin:
            data = fin.read(buffer)
            while data != b"" and not self.is_shutdown:
                # send data to docker load
                docker_load_process.stdin.write(data)
                # compute progress
                bytes_transferred += len(data)
                self._set_tick(level, bytes_transferred)
                # read more bytes
                data = fin.read(buffer)
        out, _ = docker_load_process.communicate()
        self.output[level] = out

    def _docker_image_exists(self, image):
        cmd = ["docker", "images", "--format", "{{.Repository}}:{{.Tag}}"]
        images = check_output(cmd).decode('utf-8').split("\n")
        images = set(map(lambda s: s.strip(), images))
        return image in images

    def _docker_pull_image(self, image, level=3):
        self._set_action(level, 'Pulling image: %s' % image)
        docker_pull_process = Popen(['docker', 'pull', image], stdout=PIPE)
        self._set_total(level, 1)
        self._set_tick(level, 0)
        layers = set()
        self.output[level] = ''
        for line in io.TextIOWrapper(docker_pull_process.stdout, encoding="utf-8"):
            line = line.strip()
            self.output[level] += '\n' + line
            parts = line.split(':')
            if len(parts) != 2:
                continue
            layerID = parts[0].strip()
            action = parts[1].strip()
            if len(layerID) != 12:
                continue
            if action in ['Waiting', 'Pulling fs layer']:
                layers.add(layerID)
                self._set_total(level, 2*len(layers))
            elif action in ['Download complete', 'Pull complete']:
                self._tick(level)

    def _docker_run_stack(self, stack_file, level):
        stack_name = os.path.basename(stack_file).replace('.yaml', '').replace('.yml', '')
        self._set_status(level, 'Running stack: %s' % stack_name)
        self._set_total(level, 1)
        cmd = ['docker-compose', '-p', stack_name, '--file', stack_file, 'up', '-d']
        docker_compose_up_process = Popen(cmd)
        out, err = docker_compose_up_process.communicate()
        self.output[level] = err
        self._tick(level)

    def _images_in_stack(self, stack_file):
        images = []
        yaml_content = yaml.load(open(stack_file).read())
        for service_config in yaml_content['services'].values():
            images.append(service_config['image'])
        return list(set(images))


def basenames(lst):
    return [os.path.basename(fp) for fp in lst]

def percentage(partial, total, rtype=int):
    # avoid division by zero
    if total == 0:
        total = 1
    partial = min(partial, total)
    return rtype((partial / total) * 100.0)

def list_files(lst, bullet='-', indent=1):
    if lst:
        pre = '\t' * indent + bullet + ' '
        return pre + ('\n'+pre).join(lst)
    return '\t' * indent + '(none)'

def cpu_temperature():
    temp = 0
    try:
        with open(CPU_TEMPERATURE_FILE, 'rt') as fin:
            temp = int(float(fin.read()) / 1000.0)
    except:
        pass
    return temp

def remove_file(filepath):
    print('Now removing: '+filepath)
    return os.remove(filepath)
