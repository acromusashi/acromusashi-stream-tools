# acromusashi-stream-tools
Utility tools for AcroMUSASHI Stream.
This tools contains below functions.
1.Performance improvemented storm command.
2.Topology restart and update configuration tools.
3.Config file put to storm cluster's each supervisor nodes.

## Environment

* Storm cluster installed by storm-installer.

## Installing acromusashi-stream-tools package

1.Download zip archive.  
  https://github.com/acromusashi/acromusashi-stream-tools/wiki/Download

2.Download GroovyServ archive.  
  http://kobo.github.io/groovyserv/download.html  
  groovyserv-1.0.0-bin.zip
  
```
$ unzip groovyserv-1.0.0-bin.zip
$ sudo mv groovyserv-1.0.0 /opt/storm/bin/
$ sudo ln -s /opt/storm/bin/groovyserv-1.0.0 /opt/storm/bin/groovyserv
$ sudo chmod +x /opt/storm/bin/groovyserv/bin/*
$ unzip acromusashi-stream-tools-0.1.0.zip
$ sudo mv acromusashi-stream-tools-0.1.0 /opt
$ sudo ln -s /opt/acromusashi-stream-tools-0.1.0 /opt/acromusashi-stream-tools
$ sudo chmod +x /opt/acromusashi-stream-tools/bin/*
$ sudo chmod +x /opt/acromusashi-stream-tools/groovy/bin/*
$ sudo chmod +x /opt/acromusashi-stream-tools/command/*
$ sudo mv /opt/storm/bin/storm /opt/storm/bin/storm_org
$ sudo cp -p /opt/acromusashi-stream-tools/command/storm /opt/storm/bin/
$ sudo mv /opt/acromusashi-stream-tools/groovy /opt/storm/bin/
$ sudo mv /opt/acromusashi-stream-tools/groovy-script /opt/storm/bin/
```

## Usage of Utility tools
1.Performance improvemented storm command.
  Always execute storm command same user.
  If needs to execute from multi user, use sudo.

2.Topology restart and update configuration tools.
```
$ cd /opt/acromusashi-tools/bin
$ sudo ./restart_topology [Topology Library Path] [Topology Main class] [updated topology config]

example)
$ cd /opt/acromusashi-tools/bin
$ sudo ./restart_topology /opt/storm/example-stream.jar acroquest.stream.ExampleTopology /opt/storm/conf/UpdatedConfig.yaml
```

3.Config file put to storm cluster's each supervisor nodes.
[Precondition]
- Supervisor hosts has same name user executing command user's user name.
- Supervisor hosts's password is all same.
- Destination path executing command user has write permission.
```
$ cd /opt/acromusashi-stream-tools/bin
$ ./put_config_file [Put target file path] [Put destination path] [executing tool user's password]

example)
$ cd /opt/acromusashi-stream-tools/bin
$ ./put_config_file /opt/storm/conf/UpdatedTopology.yaml /opt/storm/conf/ExampleTopology.yaml Password
```
