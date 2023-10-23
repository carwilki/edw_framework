insert into
    work.pii_dynamic_view_control
select
    "empl_protected",
    "legacy_ps2_adjusted_labor_wk",
    "legacy",
    "ps2_adjusted_labor_wk",
    "false",
    lower(
        substr (
            current_user (),
            1,
            instr (current_user (), '@') -1
        )
    ),
    lower(
        substr (
            current_user (),
            1,
            instr (current_user (), '@') -1
        )
    ),
    from_utc_timestamp (current_timestamp(), 'America/Phoenix'),
    from_utc_timestamp (current_timestamp(), 'America/Phoenix');