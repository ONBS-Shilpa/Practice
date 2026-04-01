CREATE OR REPLACE FORCE VIEW APPS.NIHGL_FUND_TYPE_V
(
   FUND,
   FUND_TYPE
)
AS
   SELECT ffv.flex_value fund, ffv.attribute2 fund_type
     FROM fnd_flex_value_sets ffvs, fnd_flex_values ffv
    WHERE     ffvs.flex_value_set_name LIKE 'GL_HHS_FUND'
          AND ffvs.flex_value_set_id = ffv.flex_value_set_id