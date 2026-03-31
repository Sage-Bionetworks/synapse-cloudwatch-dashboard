
# Synapse Cloudwatch dashboard

- configuration.py collects the metrics metadata used by Cloudwatch for a given stack and saves it  to S3
- synapse_cloudwatch_dashboard_stack.py creates a dashboard based on the metadata collected

To manually create a virtualenv on MacOS and Linux:

```
$ python3 -m venv .venv
```

After the init process completes and the virtualenv is created, you can use the following
step to activate your virtualenv.

```
$ source .venv/bin/activate
```

If you are a Windows platform, you would activate the virtualenv like this:

```
% .venv\Scripts\activate.bat
```

Once the virtualenv is activated, you can install the required dependencies.

```
$ pip install -r requirements.txt
```


Now run the `configuration.py` script, for example:

```
$ python configuration.py prod 582 582-0,582-0,582-0 myProfile
```

At this point you can now synthesize the CloudFormation template for this code.

```
$ cdk synth
cdk synth --profile --context stack= --context stack_versions= --context profile_name=
```

e.g.

```
cdk synth|deploy --profile myProfile --context stack=prod --context stack_versions=580,581,582 --context profile_name=myProfile
```

