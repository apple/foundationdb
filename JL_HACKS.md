```bash
cd /Volumes/git/foundationdb/
mkdir build
cd build
cmake -DUSE_WERROR=ON -DFORCE_BOOST_BUILD=OFF -DCMAKE_POLICY_VERSION_MINIMUM=3.5 -DCMAKE_BUILD_TYPE=Debug -G Ninja ..
ninja -j4
```


launch.json used to debug interactively from VSCode

```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Debug AsyncFileEncrypted Unit Tests",
            "type": "lldb-dap",
            "request": "launch",
            "program": "${workspaceFolder}/build/bin/fdbserver",
            "args": [
                "-r",
                "unittests",
                "-f",
                "fdbrpc/AsyncFileEncrypted"
            ],
            "stopOnEntry": false,
            "cwd": "${workspaceFolder}",
            "env": {},
            "console": "integratedTerminal"
        },
        {
            "name": "Debug fdbserver (generic)",
            "type": "lldb-dap",
            "request": "launch",
            "program": "${workspaceFolder}/build/bin/fdbserver",
            "args": [],
            "stopOnEntry": false,
            "cwd": "${workspaceFolder}",
            "env": {},
            "console": "integratedTerminal"
        }
    ]
}
```
