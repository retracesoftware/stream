# meson.build
project(
  'retracesoftware_stream', 
  'cpp',
  version: '0.2.2',
  default_options: ['cpp_std=c++20'])

# Bind to the interpreter Meson/meson-python wants us to use.
py = import('python').find_installation()

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
  'retracesoftware_stream',
  sources: ['src/objectreader.cpp',
            'src/objectwriter.cpp',
            'src/objectstream.cpp',
            'src/stack.cpp',
            'src/search.cpp',
            'src/module.cpp',
            'src/demux.cpp'],
  
  install: true,

  # If you want extra flags:
  # cpp_args: ['-O3', '-fvisibility=hidden'],
  include_directories: include_directories('common-headers/include'),
)