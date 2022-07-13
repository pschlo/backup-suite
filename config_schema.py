{
    'WebDAV Config': {
        'required': True,
        'type': 'dict',
        'schema': {
            'root url': {
                'required': True,
                'type': 'string',
            },
            'username': {
                'required': True,
                'type': 'string'
            },
            'password': {
                'required': True,
                'type': 'string'
            },
            'local root path': {
                'required': True,
                'type': 'string'
            }
        }
    }
}