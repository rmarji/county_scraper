import serverless_sdk
sdk = serverless_sdk.SDK(
    tenant_id='rmarji',
    application_name='santa-clara',
    app_uid='jbBjlP39kVx0ryLqtW',
    tenant_uid='mN7fWQ31ryJRn5fTSS',
    deployment_uid='3542ce2a-d1cc-4c83-8f28-a8ed8c11abf6',
    service_name='santa-clara',
    stage_name='dev',
    plugin_version='1.3.11'
)
handler_wrapper_kwargs = {'function_name': 'santa-clara-dev-santaclara', 'timeout': 6}
try:
    user_handler = serverless_sdk.get_user_handler('handler.lambda_handler')
    handler = sdk.handler(user_handler, **handler_wrapper_kwargs)
except Exception as error:
    e = error
    def error_handler(event, context):
        raise e
    handler = sdk.handler(error_handler, **handler_wrapper_kwargs)
