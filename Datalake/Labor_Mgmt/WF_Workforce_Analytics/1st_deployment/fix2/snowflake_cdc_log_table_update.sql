UPDATE stranger_things.snowflake_cdc_log set dlSchema = "empl_protected",dlTable ="legacy_WFA_TSCHD" where dlTable ="WFA_TSCHD";
UPDATE stranger_things.snowflake_cdc_log set dlSchema = "empl_protected",dlTable ="legacy_WFA_TDTL" where dlTable ="WFA_TDTL";
