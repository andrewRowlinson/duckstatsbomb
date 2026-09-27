create or replace secret statsbomb (
    type http,
    scope getvariable('sb_scope'),
    extra_http_headers map{'Authorization': getvariable('sb_authorization'),
                           'User-Agent': getvariable('sb_user_agent')}
);
