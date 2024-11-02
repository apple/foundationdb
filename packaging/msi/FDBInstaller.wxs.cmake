<?xml version='1.0' encoding='windows-1252'?>

<!--
  Version information must be supplied as definitions to the preprocessor.
  The two items needed are:
    Version     - in the format x.x.x.x
    VersionName - like "Beta 42"
-->

<!-- General properties -->
<?define Manufacturer='FoundationDB' ?>
<?define Title='FoundationDB' ?>
<?define Description="FoundationDB" ?>

<!-- File names -->
<?define ClusterFileName='fdb.cluster' ?>
<?define ConfFileName='foundationdb.conf' ?>
<?define LogFilePath='logs' ?>
<?define DataFilePath='data' ?>

<!-- Version -->
<?define Version='@VERSION@' ?>
<?define VersionName='@VERSION_NAME@' ?>

<!-- Paths -->
<?define fdbserver='@fdbserver@' ?>
<?define fdbcli='@fdbcli@' ?>
<?define fdbbackup='@fdbbackup@' ?>
<?define fdbmonitor='@fdbmonitor@' ?>
<?define fdbc='@fdbc@' ?>
<?define fdbclib='@fdbc_lib@' ?>
<?define SolutionRoot='@SOURCE_ROOT@' ?>
<?define BuildRoot='@BUILD_ROOT@' ?>
<?define ProjectRoot='@CURRENT_SOURCE_DIR@' ?>


<!-- Python versions that we "know" how to install -->
<?define PyPath='$(var.BuildRoot)bindings\python\fdb\'?>
<?define PyVersions='2.6;2.7;3.0;3.1;3.2;3.3;3.4;CUSTOM' ?>
<?define PyGUID2.6='{0A609E07-E0C9-4843-9DCE-DB3702B89504}' ?>
<?define PyGUID2.7='{1E993E0B-3DFD-4023-AD2A-813F32E358A8}' ?>
<!-- Note that the following 4 GUIDs are in fact not the same. They differ in their final digits. -->
<?define PyGUID3.0='{8AFAAF11-871E-43F4-BDF8-279E0E2AFC80}' ?>
<?define PyGUID3.1='{8AFAAF11-871E-43F4-BDF8-279E0E2AFC81}' ?>
<?define PyGUID3.2='{8AFAAF11-871E-43F4-BDF8-279E0E2AFC82}' ?>
<?define PyGUID3.3='{8AFAAF11-871E-43F4-BDF8-279E0E2AFC83}' ?>
<?define PyGUID3.4='{AC696FA5-05B4-4E00-BA6D-8F7F79DE4671}' ?>

<Wix xmlns='http://schemas.microsoft.com/wix/2006/wi'>
  <Product Name='$(var.Title)'
           Id='{A4228020-2D9B-43FA-B3AE-4EE6297105F5}'
           UpgradeCode='{A95EA002-686E-4164-8356-C715B7F8B1C8}'
           Version='$(var.Version)'
           Manufacturer='$(var.Manufacturer)'
           Language='1033'
           Codepage='1252' >
    <Package Id='*'
             Description='$(var.Description)'
             Manufacturer='$(var.Manufacturer)'
             InstallerVersion='200'
             Languages='1033'
             Compressed='yes'
             SummaryCodepage='1252'
             Platform='x64' />

    <Condition Message='This application is only supported on 64-bit Windows XP or higher.'>
        <![CDATA[Installed OR (VersionNT64 >= 500)]]>
    </Condition>
    <Media Id='1' Cabinet='internal.cab' EmbedCab='yes'/>

    <MajorUpgrade DowngradeErrorMessage='A later version of FoundationDB is already installed. Setup will now exit.' />

    <!-- This icon will be used in the add/remove programs menu, not on the MSI installer itself -->
    <Icon Id="fdb.ico" SourceFile="$(var.SolutionRoot)packaging\foundationdb.ico"/>
    <Property Id="ARPPRODUCTICON" Value="fdb.ico" />

    <!-- SOMEDAY: The path [CommonAppDataFolder]foundationdb\ should be the same as [CONFIGFOLDER] but
          that path did not seem to be evaluated in time for this property. If the storage folder is ever
          something that the user can set we're hosed - but for now we can construct the path from primitives.  -->
    <Property Id='CONFIGFILEEXISTS'>
      <DirectorySearch Id='CheckConfigFileDir' Path='[CommonAppDataFolder]foundationdb\'>
        <FileSearch Id='CheckConfigFile' Name='$(var.ConfFileName)' />
      </DirectorySearch>
    </Property>
    <Property Id='CLUSTERFILEEXISTS'>
      <DirectorySearch Id='CheckClusterFileDir' Path='[CommonAppDataFolder]foundationdb\'>
        <FileSearch Id='CheckClusterFile' Name='$(var.ClusterFileName)' />
      </DirectorySearch>
    </Property>

    <!-- Debug tests for file presence -->
    <!--<Condition Message='Cluster file does not exist. d[CLUSTERFILEEXISTS]b is the name'>CLUSTERFILEEXISTS</Condition>
    <Condition Message='Cluster file does exist. d[CLUSTERFILEEXISTS]b is the name'>NOT CLUSTERFILEEXISTS</Condition>
    <Condition Message='Config file does not exist. d[CONFIGFILEEXISTS]b is the name'>CONFIGFILEEXISTS</Condition>
    <Condition Message='Config file does exist. d[CONFIGFILEEXISTS]b is the name'>NOT CONFIGFILEEXISTS</Condition>-->

    <?foreach PyVer in $(var.PyVersions)?>
      <?if $(var.PyVer) != CUSTOM?>
        <Property Id='PYTARGETDIR$(var.PyVer)'>
          <RegistrySearch Id='PythonMachine$(var.PyVer)' Root='HKLM' Key='SOFTWARE\Python\PythonCore\$(var.PyVer)\InstallPath'
                          Type='directory' Win64='yes' />
        </Property>
      <?endif?>
    <?endforeach?>

    <!-- Debug tests for python presence -->
    <!--<Condition Message='Py2.7 does not exist. '[PYTARGETDIR2.7]' is the name'>PYTARGETDIR2.7</Condition>
    <Condition Message='Py2.7 does exist. '[PYTARGETDIR2.7]' is the name'>NOT PYTARGETDIR2.7</Condition>-->

    <Directory Id='TARGETDIR' Name='SourceDir'>
      <Component Id='InvisibleDummyComponent' Guid='{4835E5C0-70B6-4341-9A4F-82859DF92A2F}'/>
      <Directory Id='ProgramFiles64Folder' Name='PFiles'>
        <Directory Id='INSTALLDIR' Name='foundationdb'>
          <Component Id='InstallPathEnvVar' Guid='{D6DDFEEC-154E-4684-BCBE-D079BB121B1B}' Win64='yes'>
            <CreateFolder />
                  <Environment Action='set' Name='FOUNDATIONDB_INSTALL_PATH' Value='[INSTALLDIR]' Part='all' Id='InstallPathSetEnvVar' System='yes' Permanent='no' />
          </Component>
          <Directory Id='IncludeDir' Name='include'>
            <Directory Id='IncludeFDBDir' Name='foundationdb'>
              <Component Id='FDBCLibraryHeader' Guid='{32D846FA-3BA8-4CF6-8777-51DFA1011198}' Win64='yes'>
                <File Id='FDBCH' Name='fdb_c.h' DiskId='1' Source='$(var.SolutionRoot)bindings\c\foundationdb\fdb_c.h' KeyPath='yes'/>
                <File Id='FDBCOPTIONSH' Name='fdb_c_options.g.h' DiskId='1' Source='$(var.BuildRoot)bindings\c\foundationdb\fdb_c_options.g.h'/>
                <File Id='FDBOPTIONSFILE' Name='fdb.options' DiskId='1' Source='$(var.SolutionRoot)fdbclient\vexillographer\fdb.options'/>
              </Component>
            </Directory>
          </Directory>
          <Directory Id='LibDir' Name='lib'>
            <Directory Id='LibFDBDir' Name='foundationdb'>
              <Component Id='FDBCLibraryLib' Guid='{45503C5E-0A84-461D-9803-ABBDB004F1A4}' Win64='yes'>
                <File Id='FDBLIB' Name='fdb_c.lib' DiskId='1' Source='$(var.fdbclib)' KeyPath='yes'/>
              </Component>
            </Directory>
          </Directory>
          <Directory Id='BinDir' Name='bin'>
            <Component Id='PathAddition' Guid='{3BC6E7A3-D83F-40CF-9209-3BA249DD2C15}' Win64='yes'>
              <CreateFolder />
              <Environment Action='set' Name='PATH' Value='[INSTALLDIR]bin' Part='last' Id='PathSetAddition' System='yes' Permanent='no'/>
            </Component>

            <Component Id='FDBServerExecutable' Guid='{C92DFCC9-BB95-4B13-8284-6DDDB3290D70}' Win64='yes'>
              <File Id='FDBServerEXE' Name='fdbserver.exe' DiskId='1' Source='$(var.fdbserver)' KeyPath='yes'/>
            </Component>

            <Component Id='FDBBackupAgentExecutable' Guid='{C92DFCC9-BB95-4B13-8284-6DDDB3290D71}' Win64='yes'>
              <File Id='FDBBackupAgentEXE' Name='backup_agent.exe' DiskId='1' Source='$(var.fdbbackup)' KeyPath='yes'/>
            </Component>

            <Component Id='FDBCLIExecutable' Guid='{F5E0A796-EC21-406F-88E1-3195165E3077}' Win64='yes'>
              <File Id='FDBCLIEXE' Name='fdbcli.exe' DiskId='1' Source='$(var.fdbcli)' KeyPath='yes'/>
            </Component>

            <Component Id='FDBCRegistryValue' Guid='{6ED940F3-75C8-4385-97D9-D7D0F211B17D}' Win64='yes'>
              <RegistryKey Root='HKLM' Key='SOFTWARE\$(var.Manufacturer)\KeyValue\Client'>
                <RegistryValue Name='Version' Type='string' Value='$(var.Version)' KeyPath='yes' />
              </RegistryKey>
            </Component>

            <Component Id='FDBSRegistryValue' Guid='{361A9B4A-A06F-4BFB-AFEA-B5F733C8BFDF}' Win64='yes'>
              <RegistryKey Root='HKLM' Key='SOFTWARE\$(var.Manufacturer)\KeyValue\Server'>
                <RegistryValue Name='Version' Type='string' Value='$(var.Version)' KeyPath='yes' />
              </RegistryKey>
            </Component>

            <Component Id='FDBBackupClientExecutable' Guid='{F5E0A796-EC21-406F-88E1-3195165E3078}' Win64='yes'>
              <File Id='FDBBACKUPEXE' Name='fdbbackup.exe' DiskId='1' Source='$(var.fdbbackup)' KeyPath='yes'/>
              <File Id='FDBRESTOREEXE' Name='fdbrestore.exe' DiskId='1' Source='$(var.fdbbackup)' KeyPath='no'/>
              <File Id='FDBDREXE' Name='fdbdr.exe' DiskId='1' Source='$(var.fdbbackup)' KeyPath='no'/>
            </Component>

            <Component Id='FDBCLibraryPFiles' Guid='{6184C0C5-D29F-48DD-BE62-4776EB79C34A}' Win64='yes'>
              <File Id='FDBCDLL' Name='fdb_c.dll' DiskId='1' Source='$(var.fdbc)' KeyPath='yes'/>
            </Component>

            <Component Id='fdbmonitorExecutable' Guid='{D62DAD0B-A0AF-44EA-B2E0-28AD7ADBC29B}' Win64='yes'>
              <ServiceControl Id='fdbmonitorStop' Stop='both' Name='fdbmonitor' Remove='both' Wait='yes'/>
              <File Id='fdbmonitorEXE' Name='fdbmonitor.exe' DiskId='1' Source='$(var.fdbmonitor)' KeyPath='yes'/>
              <ServiceInstall Id='fdbmonitorInstall' Name='fdbmonitor' DisplayName='FoundationDB Server Monitor (fdbmonitor)'
                              Description='Starts and monitors instances of the FoundationDB server' Start='auto'
                              Vital='yes' ErrorControl='normal' Interactive='no' Type='ownProcess'/>
              <ServiceControl Id='fdbmonitorStart' Start='install' Name='fdbmonitor' Wait='yes'/>
            </Component>

          </Directory>
        </Directory>
      </Directory>
      <Directory Id='CommonAppDataFolder' Name='AppData'>
        <Directory Id='CONFIGFOLDER' Name='foundationdb'>
          <Component Id='CreateClusterFileDir' Guid='{165AA4E7-6D5B-4974-A2C8-6AF6E4C9295C}' Permanent='yes'>
            <CreateFolder/>
          </Component>

          <!-- Copy default config file to seed conf file. Executed only if there is no config file present -->
          <Component Id='fdbmonitorConfigFile' Guid='{40D6746D-1433-4BD9-A4F6-E15335D7BBB4}' Permanent='yes'>
            <CreateFolder />
            <File Id='fdbmonitorConfigFileCreate' Name='$(var.ConfFileName)' DiskId='1' Source='$(var.ProjectRoot)\skeleton.conf' KeyPath='yes'/>
          </Component>

          <!-- Edit config file to point to binary (and add data, etc to default file). Executed always on install -->
          <Component Id='fdbmonitorConfigReplaceBinary' Guid='{97A2CD69-3D8A-442F-83CD-D5BAF0C50D37}' Permanent='yes'>
            <CreateFolder />
            <IniFile Id='WriteIntoIniFileCluster' Action='createLine' Key='cluster_file' Directory='CONFIGFOLDER'
                     Name='$(var.ConfFileName)' Section='general' Value='[CONFIGFOLDER]$(var.ClusterFileName)' />
            <!-- Remove "binary" line from old installations -->
            <IniFile Id='ClearFromIniFileBinary' Action='removeLine' Key='binary' Directory='CONFIGFOLDER'
                     Name='$(var.ConfFileName)' Section='fdbserver'/>
            <!-- Update "command" line even if changed from original -->
            <IniFile Id='WriteIntoIniFileBinary' Action='addLine' Key='command' Directory='CONFIGFOLDER'
                     Name='$(var.ConfFileName)' Section='fdbserver' Value='[INSTALLDIR]bin\fdbserver.exe' />
            <IniFile Id='WriteIntoIniFileData' Action='createLine' Key='datadir' Directory='CONFIGFOLDER'
                     Name='$(var.ConfFileName)' Section='fdbserver' Value='[CONFIGFOLDER]$(var.DataFilePath)\$ID' />
            <IniFile Id='WriteIntoIniFileLogs' Action='createLine' Key='logdir' Directory='CONFIGFOLDER'
                     Name='$(var.ConfFileName)' Section='fdbserver' Value='[CONFIGFOLDER]$(var.LogFilePath)' />
            <!-- parentpid is in the skeleton, but we add it here for old installations -->
            <IniFile Id='WriteIntoIniFileParentPid' Action='createLine' Key='parentpid' Directory='CONFIGFOLDER'
                     Name='$(var.ConfFileName)' Section='fdbserver' Value='$PID' />
            <!-- Update backup-agent "command" line even if changed from original -->
            <IniFile Id='WriteIntoIniFileBackupAgent' Action='addLine' Key='command' Directory='CONFIGFOLDER'
                     Name='$(var.ConfFileName)' Section='backup-agent' Value='[INSTALLDIR]bin\backup_agent.exe' />
            <IniFile Id='WriteIntoBackupIniFileLogs' Action='createLine' Key='logdir' Directory='CONFIGFOLDER'
                     Name='$(var.ConfFileName)' Section='backup-agent' Value='[CONFIGFOLDER]$(var.LogFilePath)' />
          </Component>

          <Directory Id='DATADIR' Name='$(var.DataFilePath)'>
            <Component Id='CreateDataDir' Guid='{242DD218-BC97-4217-98A9-A9E9A2B780D0}' Permanent='yes'>
              <CreateFolder />
            </Component>
          </Directory>

          <Directory Id='LOGDIR' Name='$(var.LogFilePath)'>
            <Component Id='CreateLogDir' Guid='{C2743E29-1B7B-46E9-9B02-DD80842D7B27}' Permanent='yes'>
              <CreateFolder />
            </Component>
          </Directory>

        </Directory>
      </Directory>

      <Directory Id="WINDOWSVOLUME" Name="CCOLON">
      <?foreach PyVer in $(var.PyVersions)?>
        <?if $(var.PyVer) = CUSTOM?>
          <?define PyTopDir='Python' ?>
        <?else?>
          <?define PyTopDir='Python$(var.PyVer)' ?>
        <?endif?>

        <Directory Id='PYTARGETDIR$(var.PyVer)' Name='$(var.PyTopDir)'>
          <Directory Id='PyTarget$(var.PyVer)Lib' Name='Lib'>
            <Directory Id='PyTarget$(var.PyVer)Site' Name='site-packages'>
              <Directory Id='PyTarget$(var.PyVer)PackageDir' Name='fdb'>
                <Directory Id='PyTarget$(var.PyVer)CacheDir' Name='__pycache__'></Directory>

                <?if $(var.PyVer) = 2.6?>
                  <?define PyGUID='$(var.PyGUID2.6)' ?>
                <?endif?>
                <?if $(var.PyVer) = 2.7?>
                  <?define PyGUID='$(var.PyGUID2.7)' ?>
                <?endif?>
                <?if $(var.PyVer) = 3.0?>
                <?define PyGUID='$(var.PyGUID3.0)' ?>
                <?endif?>
                <?if $(var.PyVer) = 3.1?>
                <?define PyGUID='$(var.PyGUID3.1)' ?>
                <?endif?>
                <?if $(var.PyVer) = 3.2?>
                <?define PyGUID='$(var.PyGUID3.2)' ?>
                <?endif?>
                <?if $(var.PyVer) = 3.3?>
                <?define PyGUID='$(var.PyGUID3.3)' ?>
                <?endif?>
                <?if $(var.PyVer) = 3.4?>
                <?define PyGUID='$(var.PyGUID3.4)' ?>
                <?endif?>
                <?if $(var.PyVer) = CUSTOM?>
                  <!-- Since there is no GUID for the Custom install-->
                  <?define PyGUID='' ?>
                <?endif?>

                <Component Id='PyTarget$(var.PyVer)Install' Guid='$(var.PyGUID)'>
                  <CreateFolder />
                  <File Id='FDBPY$(var.PyVer)IMPL' Name='impl.py' DiskId='1' Source='$(var.PyPath)impl.py' KeyPath='yes'/>
                  <File Id='FDBPY$(var.PyVer)INIT' Name='__init__.py' DiskId='1' Source='$(var.PyPath)__init__.py' KeyPath='no'/>
                  <File Id='FDBPY$(var.PyVer)OPTS' Name='fdboptions.py' DiskId='1' Source='$(var.PyPath)fdboptions.py' KeyPath='no'/>
                  <File Id='FDBPY$(var.PyVer)LOCA' Name='locality.py' DiskId='1' Source='$(var.PyPath)locality.py' KeyPath='no'/>
                  <File Id='FDBPY$(var.PyVer)TUPLE' Name='tuple.py' DiskId='1' Source='$(var.PyPath)tuple.py' KeyPath='no'/>
                  <File Id='FDBPY$(var.PyVer)DIR' Name='directory_impl.py' DiskId='1' Source='$(var.PyPath)directory_impl.py' KeyPath='no'/>
                  <File Id='FDBPY$(var.PyVer)SUBS' Name='subspace_impl.py' DiskId='1' Source='$(var.PyPath)subspace_impl.py' KeyPath='no'/>
                  <RemoveFile Id="Purge$(var.PyVer)PYC" Name="*.pyc" On="uninstall" />
                  <RemoveFile Id="Purge$(var.PyVer)PYO" Name="*.pyo" On="uninstall" />
                  <RemoveFile Id="Purge$(var.PyVer)Cache" Directory="PyTarget$(var.PyVer)CacheDir" Name="*.*" On="uninstall" />
                  <RemoveFolder Id="Purge$(var.PyVer)CacheDir" Directory="PyTarget$(var.PyVer)CacheDir" On="uninstall" />
                </Component>
                <?undef PyGUID?>
              </Directory>
            </Directory>
          </Directory>
        </Directory>
        <?undef PyTopDir?>
      <?endforeach?>
      </Directory>
    </Directory>

    <Feature Id='FeatureRoot' Title='FoundationDB $(var.VersionName)'
             Description='Installs the server and client libraries for FoundationDB $(var.VersionName)' Level='1'
             Absent='disallow' AllowAdvertise='no' Display='expand' TypicalDefault='install'
             ConfigurableDirectory='INSTALLDIR'>

      <ComponentRef Id='InvisibleDummyComponent'/> <!-- Having any component here suppresses network install option in menu. -->

      <Feature Id='FeatureClient' Title='FoundationDB Clients'
               Description='Installs the FoundationDB command line interface, client library (required by all bindings) and C header files.'
               Level='1' Absent='disallow' AllowAdvertise='no' TypicalDefault='install'>
        <!-- This is the equivalent of the 'clients' package on linux -->
        <ComponentRef Id='CreateClusterFileDir'/>  <!-- In a client only install, we don't make any files here, but want it to be easy to drop fdb.cluster here -->
        <ComponentRef Id='PathAddition'/>
        <ComponentRef Id='InstallPathEnvVar'/>
        <ComponentRef Id='FDBCLibraryPFiles' />
        <ComponentRef Id='FDBCLibraryHeader'/>
        <ComponentRef Id='FDBCLibraryLib' />
        <ComponentRef Id='FDBCLIExecutable' />
        <ComponentRef Id='FDBBackupClientExecutable' />
        <ComponentRef Id='FDBCRegistryValue' />
      </Feature>

      <Feature Id='FeatureServer' Title='FoundationDB Server'
               Description='Installs the FoundationDB server (fdbserver) and fdbmonitor service. A standalone development server will be started automatically.'
               Level='1' Absent='allow' AllowAdvertise='no' Display='collapse' TypicalDefault='install'>

        <ComponentRef Id='FDBServerExecutable' />
        <ComponentRef Id='FDBBackupAgentExecutable' />

        <Feature Id='FeatureService' Title='Windows Service'
                 Description='Background service to run and manage FoundationDB server instances' Level='1'
                 Absent='disallow' AllowAdvertise='no' Display='hidden' InstallDefault='followParent'>
          <Feature Id='FeatureServiceFileNew' Title='Config File Creation' Display='hidden' Level='1' >
            <ComponentRef Id='fdbmonitorConfigFile' />
            <!-- Set this to 0 (i.e. disabled) if the config file already exists -->
            <Condition Level='0'>CONFIGFILEEXISTS</Condition>
          </Feature>
          <ComponentRef Id='fdbmonitorConfigReplaceBinary' />
          <ComponentRef Id='CreateLogDir' />
          <ComponentRef Id='CreateDataDir' />
          <ComponentRef Id='fdbmonitorExecutable' />
          <ComponentRef Id='FDBSRegistryValue' />
        </Feature>
      </Feature>

      <Feature Id='FeaturePython' Title='Python Language Bindings'
                Description='Install FoundationDB bindings into the default packages for specific installed copies of Python'
                Level='1' Absent='allow' TypicalDefault='install' AllowAdvertise='no' >

        <ComponentRef Id='InvisibleDummyComponent'/>  <!-- Having any component here suppresses network install option in menu. -->

        <?foreach PyVer in $(var.PyVersions)?>
          <?if $(var.PyVer) != CUSTOM?>
            <Feature Id='FeaturePython$(var.PyVer)' Title='Python $(var.PyVer)'
                      Description='Install FoundationDB bindings into Python $(var.PyVer) (from registry)'
                      Level='0' Absent='allow' TypicalDefault='install' AllowAdvertise='no' >
              <ComponentRef Id='PyTarget$(var.PyVer)Install' />
              <Condition Level='1'>PYTARGETDIR$(var.PyVer)</Condition>
            </Feature>
          <?endif?>
        <?endforeach?>
        <Feature Id='FeaturePythonCUSTOM' Title='Custom Python Installation'
                  Description='Install FoundationDB bindings into a non default Python installation'
                  Level='999' Absent='allow' TypicalDefault='install' AllowAdvertise='no'
                  ConfigurableDirectory='PYTARGETDIRCUSTOM' >
          <ComponentRef Id='PyTargetCUSTOMInstall' />
        </Feature>
      </Feature>
    </Feature>

    <?foreach PyVer in $(var.PyVersions)?>
      <CustomAction Id='CompilePythonFiles$(var.PyVer)' Directory='PYTARGETDIR$(var.PyVer)'
                    ExeCommand='"[PYTARGETDIR$(var.PyVer)]python.exe" -m compileall "[PyTarget$(var.PyVer)Site]fdb"'
                    Execute='deferred'
                    Impersonate='no'
                    Return='ignore' />
    <?endforeach?>

    <Property Id='CMDEXE'>cmd.exe</Property>
    <CustomAction Id='CreateRandomClusterFile' Property='CMDEXE'
                  ExeCommand='/V:ON /C "echo %RANDOM%%RANDOM%%RANDOM%%RANDOM%%RANDOM%%RANDOM%:%RANDOM%%RANDOM%%RANDOM%%RANDOM%%RANDOM%%RANDOM%@127.0.0.1:4500 > "[CONFIGFOLDER]$(var.ClusterFileName)""'
                  Execute='deferred'
                  Return='check' />

    <CustomAction Id='ConfigureNewDatabase' Property='CMDEXE'
                  ExeCommand='/V:ON /C ""[INSTALLDIR]bin\fdbcli.exe" -C "[CONFIGFOLDER]$(var.ClusterFileName)" --exec "configure new single memory; status" --timeout 30"'
                  Execute='deferred'
                  Return='check' />

    <InstallExecuteSequence>
      <!-- Compile .pyc files for any python versions we are installing to -->
      <?foreach PyVer in $(var.PyVersions)?>
      <Custom Action='CompilePythonFiles$(var.PyVer)' Before='StartServices'>&amp;FeaturePython$(var.PyVer)=3</Custom>
      <?endforeach?>

      <!-- Configure the new cluster when ServiceFeature is being installed and not on an upgrade -->
      <!-- See the totally unexplained list of conditions at http://msdn.microsoft.com/en-us/library/aa368561%28VS.85%29.aspx -->
      <Custom Action='CreateRandomClusterFile' Before='StartServices'>(&amp;FeatureService=3) AND NOT(!FeatureService=3) AND (NOT CLUSTERFILEEXISTS)</Custom>
      <Custom Action='ConfigureNewDatabase' After='StartServices'>(&amp;FeatureService=3) AND NOT(!FeatureService=3) AND (NOT CLUSTERFILEEXISTS)</Custom>
    </InstallExecuteSequence>

    <Property Id="WIXUI_EXITDIALOGOPTIONALTEXT"
              Value="Thank you for installing FoundationDB. For documentation, please visit https://apple.github.io/foundationdb/index.html#documentation.

To allow path variables to update, please restart your IDE and any open terminal sessions." />

    <UIRef Id='WixUI_FeatureTree' />
    <UI>
      <Publish Dialog="WelcomeDlg"
            Control="Next"
            Event="NewDialog"
            Value="FeaturesDlg"
            Order="2">1</Publish>
      <Publish Dialog="FeaturesDlg"
            Control="Back"
            Event="NewDialog"
            Value="WelcomeDlg"
            Order="2">1</Publish>
    </UI>
    <UIRef Id='WixUI_ErrorProgressText' />

    <WixVariable Id="WixUIDialogBmp" Value="$(var.ProjectRoot)\art\dialog.jpg" />
    <WixVariable Id="WixUIBannerBmp" Value="$(var.ProjectRoot)\art\banner.jpg" />
  </Product>
</Wix>
