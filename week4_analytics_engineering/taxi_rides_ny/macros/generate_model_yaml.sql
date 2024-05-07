{% set model_to_generate=codegen.get_models(directory='staging', prefix='stg_') %}
{{
    codegen.generate_model_yaml(
        model_names = model_to_generate
    )
}}