create secret http_proxy (
    type http,
    http_proxy $url,
    http_proxy_username $username,
    http_proxy_password $password
);