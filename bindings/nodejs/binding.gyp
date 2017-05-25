{
  'targets': [
    {
      'target_name': 'fdblib',
      'sources': [ 'src/FdbV8Wrapper.cpp', 'src/Database.cpp', 'src/Transaction.cpp', 'src/Cluster.cpp', 'src/FdbError.cpp', 'src/FdbOptions.cpp', 'src/FdbOptions.g.cpp' ],
      'include_dirs': ['../c'],
      'conditions': [
        ['OS=="linux"', {
          'link_settings': { 'libraries': ['-lfdb_c', '-L../../../lib'] },
        }],
        ['OS=="mac"', {
          'xcode_settings': { 
            'MACOSX_DEPLOYMENT_TARGET': '10.7',       # -mmacosx-version-min=10.7
            'OTHER_CFLAGS': ['-std=c++0x'] 
          },
          'link_settings': { 'libraries': ['-lfdb_c', '-L../../../lib'] },
        }],
        ['OS=="win"', {
          'link_settings': { 'libraries': ['../../../bin/Release/fdb_c.lib'] },
        }],
      ],
      'cflags': ['-std=c++0x'],
    }
  ]
}
