### DAS LifeCycle Agent

This section conatins code for DAS LifeCycle Agent.
It consists of three actions:

- das-queries.py, module which prepare DAS queries
- das-request.py, module which sends DAS requests to DAS server
- das-results.py, module which collects and process DAS requests

To run DAS LifeCycle workflow, please setup LifeCycle environment [1] and run
it as following:

./Lifecycle.pl --config <path>/DAS.conf

On lxplus you may use Tony's setup:

```
cd workdir
# get LifeCycle code
git clone git@github.com:dmwm/LifeCycle.git
# get local copy of DAS workflow
cp -r LifeCycle/DAS .
# copy Tony's LifeCycle testbed
tpublic=/afs/cern.ch/user/w/wildish/public
export PERL5LIB=$tpublic/COMP/PHEDEX_GIT/perl_lib:$tpublic/COMP/T0_CVS/perl_lib:$tpublic/perl:$tpublic/perl/lib:$tpublic/perl/lib/arch:$tpublic/perl/lib/perl5
cp -r $tpublic/COMP/PHEDEX_GIT/Testbed/LifeCycle ./LifeCycleTestbed
cp -r LifeCycle/DAS LifeCycleTestbed
cd LifeCycletestbed
# run DAS workflow
./Lifecycle.pl --config DAS/DAS.conf
```

[1] https://twiki.cern.ch/twiki/bin/viewauth/CMS/PhedexProjLifeCycleTestbed
