# meson.build - Project configuration + shared build logic

project(
  '_retracesoftware_stream',
  'cpp',
  version: run_command('python', '-m', 'setuptools_scm', check: true, capture: true).stdout().strip(),
  default_options: ['cpp_std=c++20', 'strip=false'])

# Project-specific configuration
rs_extension_name = '_retracesoftware_stream'
rs_cpp_source_dir = 'cpp'
rs_ext_install_subdir = ''

# Custom Python installs (different destinations)
py = import('python').find_installation(pure: false)
py.install_sources('src/retracesoftware/stream.py', subdir: 'retracesoftware')
py.install_sources('src/retracesoftware/stream/__init__.py', subdir: 'retracesoftware/stream')

# Include shared build logic (builds release/debug modules)
subdir('common-headers/meson')
