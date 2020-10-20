[Device42](http://www.device42.com/) is a Continuous Discovery software for your IT Infrastructure. It helps you automatically maintain an up-to-date inventory of your physical, virtual, and cloud servers and containers, network components, software/services/applications, and their inter-relationships and inter-dependencies.


This repository contains script that helps you sync data from Device42 to FreshService.

### Download and Installation
-----------------------------
Device42 v16.19.00+ (Legacy Included)
Python 3.5+

The following Python Packages are required:

* pycrypto==2.6.1
* pyparsing==2.1.10
* pyzmq==16.0.2
* requests==2.13.0
* xmljson==0.2.0

These can all be installed by running `pip install -r requirements.txt`.

Once installed, the script itself is run by this command: `python d42_sd_sync.py`.


### Download and Installation (Legacy)
----------------------------- 
To utilize the Device42_freshservice_mapping script, Python 3.5+ is required. The following Python Packages are required as well:

pycrypto==2.6.1
pyparsing==2.1.10
pyzmq==16.0.2
requests==2.13.0
xmljson==0.2.0
These can all be installed by running pip install -r requirements.txt.

In order to run the legacy migration, you will also need to modify the mapping.xml file so that the legacy mapping options are used

modify the following line so that `enable` is set to false for the v2_views
```enable="false" description="Copy Servers from Device42 to FreshService using DOQL v2_views"```

modify the following line so that `enable` is set to true for the v1_views
```enable="true" description="Copy Servers from Device42 to FreshService using DOQL v1_views"```

Once the packages are installed and the script is configured, the script can be run by this command: python d42_sd_sync.py.

### Configuration
-----------------------------
Prior to using the script, it must be configured to connect to your Device42 instance and your FreshService instance.
* Save a copy of mapping.xml.sample as mapping.xml. 
* Enter your URL, User, Password, API Key in the FreshService and Device42 sections (lines 2-10).
API Key can be obtained from FreshService profile page

Below the credential settings, you’ll see a Tasks section. 
Multiple Tasks can be setup to synchronize various CIs from Device42 to FreshService.
In the <api> section of each task, there will be a <resource> section that queries Device42 to obtain the desired CIs. 
Full documentation of the Device42 API and endpoints is available at https://api.device42.com. 
Individual tasks within a mapping.xml file can be enabled or disabled at will by changing the `enable="true"` to `enable="false"` in the <task> section.

Once the Device42 API resource and FreshService Target are entered, the <mapping> section is where fields from Device42 (the `resource` value) can be mapped to fields in FreshService (the `target` value).
It is very important to adjust the list of default values in accordance between freshservice and device 42 (for example, service_level).

After configuring the fields to map as needed, the script should be ready to run. 

### Gotchas
-----------------------------
* FreshService API Limit is 1000 calls per hour (https://api.freshservice.com/#ratelimit)
* Due to the nature of FreshService rate limits, large inventories may take extended periods of time to migrate

Please use the following table as a reference only, actual times may vary due to request limit cooldowns and other internal API calls

|# of Devices| Migration Time|
|------------|---------------|
| 100   | 6 min |
| 1,000 | 1 hr |
| 5,000 | 5 hrs | 
|10,000 | 10 hrs |
|24,000 | 24 hrs |


### Compatibility
-----------------------------
* Script runs on Linux and Windows

### Info
-----------------------------
* mapping.xml - file from where we get fields relations between D42 and FreshService
* devicd42.py - file with integration device42 instance
* freshservice.py - file with integration freshservice instance
* d42_sd_sync.py - initialization and processing file, where we prepare API calls

### Support
-----------------------------
We will support any issues you run into with the script and help answer any questions you have. Please reach out to us at support@device42.com

### Version
-----------------------------
1.0.0.190411