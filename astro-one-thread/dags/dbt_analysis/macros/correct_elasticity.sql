{% macro correct_elasticity(elasticity, correction_factor) %}
    (ABS({{elasticity}} * {{correction_factor}}) + {{elasticity}})
{% endmacro %}