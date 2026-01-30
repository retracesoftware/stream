# meson.build
project(
  'retracesoftware_stream', 
  'cpp',
  version: run_command('python', '-m', 'setuptools_scm', check: true, capture: true).stdout().strip(),
  default_options: ['cpp_std=c++20'])

# Bind to the interpreter Meson/meson-python wants us to use.
py = import('python').find_installation()

# Debug build option - when true, builds retracesoftware_stream_debug
debug_build = get_option('debug_build')

if debug_build
  module_name = 'retracesoftware_stream_debug'
  extra_cpp_args = ['-DDEBUG_BUILD=1']
else
  module_name = 'retracesoftware_stream'
  extra_cpp_args = []
endif

install_subdir('proxy', install_dir: py.get_install_dir() / 'retracesoftware')

#  'proxy/__init__.py',
#  'proxy/install.py',
#  'proxy/proxyfactory.py',
#  'proxy/record.py',
#  'proxy/replay.py',
#  subdir: 'retracesoftware'

py.install_sources(
  'src/stream.py',
  subdir: 'retracesoftware')

py.extension_module(
  module_name,
  sources: ['src/objectreader.cpp',
            'src/objectwriter.cpp',
            'src/objectstream.cpp',
            'src/stack.cpp',
            'src/search.cpp',
            'src/module.cpp',
            'src/demux.cpp'],
  
  install: true,

  cpp_args: extra_cpp_args,
  include_directories: include_directories('common-headers/include'),
)