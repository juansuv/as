from rest_framework import serializers

from normaliza.models import TipoFuente, ApiCatalogo, ApiUsuarioParametro, \
    ParametrosNorm, ParametrosBD, Normalizacion, Columna, Filtro, Semana


class TipoFuenteSerializer(serializers.ModelSerializer):
    """Serializer para tipo de fuetes"""

    class Meta:
        model = TipoFuente
        fields = ('id', 'nombre')
        read_only_fields = ('id',)


class ApiCatalogoSerializer(serializers.ModelSerializer):
    """Serializer para Api Catalogo"""

    class Meta:
        model = ApiCatalogo
        fields = ('id', 'nombre')
        read_only_fields = ('id',)


class ApiUsuarioParamSerializer(serializers.ModelSerializer):
    """Serialize a Api User"""
    Api_catalogo = serializers.PrimaryKeyRelatedField(
        many=False,
        queryset=ApiCatalogo.objects.all()
    )

    class Meta:
        model = ApiUsuarioParametro
        fields = (
            'id', 'Api_catalogo',
        )
        read_only_fields = ('id',)


class ApiUsuarioParamDetailSerializer(ApiUsuarioParamSerializer):
    """Serialize a Api User detail"""
    Api_catalogo = ApiCatalogoSerializer(many=False, read_only=True)


class ParamNormSerializer(serializers.ModelSerializer):
    """Serialize a ParamNorm"""
    ApiCatalogo = serializers.PrimaryKeyRelatedField(
        many=False,
        queryset=ApiCatalogo.objects.all()
    )
    Fuente = serializers.PrimaryKeyRelatedField(
        many=False,
        queryset=TipoFuente.objects.all()
    )

    class Meta:
        model = ParametrosNorm
        fields = (
            'id', 'ApiCatalogo', 'Fuente', 'nombre', 'parametrosbd', 'nombre_fuente', 'nombre_destino'
        )
        read_only_fields = ('id',)


class ParamNormDetailSerializer(ParamNormSerializer):
    """Serialize a ParamNorm detail"""
    ApiCatalogo = ApiCatalogoSerializer(many=False, read_only=True)
    Fuente = TipoFuenteSerializer(many=False, read_only=True)


class ParamNormPostSerializer(ParamNormSerializer):
    """Serialize a ParamNorm detail"""
    ApiCatalogo = ApiCatalogoSerializer(many=False)
    Fuente = TipoFuenteSerializer(many=False)


class ParamBDSerializer(serializers.ModelSerializer):
    """Serialize a ParamBD"""
    parametrosnorm = serializers.PrimaryKeyRelatedField(
        many=False,
        queryset=ParametrosNorm.objects.all()
    )

    class Meta:
        model = ParametrosBD
        fields = (
            'id',
            'parametrosnorm',
            'protocolo',
            'host',
            'puerto',
            'SID',
            'ruta',
            'usuario',
            'password',
        )
        read_only_fields = ('id',)


class ParamBDDetailSerializer(ParamBDSerializer):
    """Serialize a ParamBD detail"""
    parametrosnorm = ParamNormSerializer(many=False, read_only=True)


class ColumnaSerializer(serializers.ModelSerializer):
    """Serialize a ParamBD"""
    normalizacion = serializers.PrimaryKeyRelatedField(
        many=False,
        queryset=Normalizacion.objects.all()
    )

    class Meta:
        model = Columna
        fields = (
            'id',
            'nombre_columna',
            'normalizacion',
            'mapeo',
            'tipo',
            'filtros',
            'tipo_columna'
        )
        read_only_fields = ('id',)


class FiltroColSerializer(serializers.ModelSerializer):
    """Serialize a ParamBD"""

    class Meta:
        model = Filtro
        fields = (
            'id',
            'operador',
            'tipo_valor',
            'valor_filtro',
        )
        read_only_fields = ('id',)


class ColumnaSerializerNoNorm(serializers.ModelSerializer):
    """Serialize a ParamBD"""
    filtros = FiltroColSerializer(many=True)

    class Meta:
        model = Columna
        fields = (
            'id',
            'nombre_columna',
            'mapeo',
            'tipo',
            'filtros',
            'tipo_columna'
        )
        read_only_fields = ('id',)

    def validate(self, data):
        if data['tipo'] > 4:
            raise serializers.ValidationError("Tipo de dato no valido")
        return data


class NormalizacionSerializer(serializers.ModelSerializer):
    """Serialize a ParamBD"""
    api_paramNorm = serializers.PrimaryKeyRelatedField(
        many=False,
        queryset=ParametrosNorm.objects.all()
    )

    class Meta:
        model = Normalizacion
        fields = (
            'id',
            'api_paramNorm',
            'nombre_fuente',
            'fecha_inicio',
            'fecha_fin',
            'periodo_salida',
            'prom_periodo',
            'vector_promedio',
            'semanas',
            'meses',
            'uuid',
            'num_process',
            'num_chunck',
            # 'valor',
            # 'tiempo',
            'columnas',
        )
        read_only_fields = ('id',)


class FiltroSerializer(serializers.ModelSerializer):
    """Serialize a ParamBD"""
    columna = serializers.PrimaryKeyRelatedField(
        many=False,
        queryset=Columna.objects.all()
    )

    class Meta:
        model = Filtro
        fields = (
            'id',
            'columna',
            'operador',
            'tipo_valor',
            'valor_filtro',
        )
        read_only_fields = ('id',)


class FiltroDetailSerializer(FiltroSerializer):
    """Serialize a ParamBD detail"""


class ColumnaDetailSerializer(ColumnaSerializerNoNorm):
    """Serialize a ParamBD detail"""
    filtros = FiltroDetailSerializer(many=True, read_only=True)


class ApiParaNormPostSerializer(ParamNormSerializer):
    """Serialize a ParamNorm detail"""
    ApiCatalogo = ApiCatalogoSerializer(many=False)
    Fuente = TipoFuenteSerializer(many=False)

    class Meta:
        model = ApiCatalogo
        fields = (
            'ApiCatalogo',
            'Fuente',
            'nombre'
        )


class SemanaNoNormSerializer(serializers.ModelSerializer):
    """Serialize a ParamBD"""

    class Meta:
        model = Semana
        fields = (
            'id',
            'periodo',
            'semanas',

        )
        read_only_fields = ('id',)


class NormalizacionPostSerializer(NormalizacionSerializer):
    columnas = ColumnaSerializerNoNorm(many=True)
    semanas_esp = SemanaNoNormSerializer(many=True)

    class Meta:
        model = Normalizacion
        fields = (
            'id',
            'api_paramNorm',
            'nombre_fuente',
            'fecha_inicio',
            'fecha_fin',
            'periodo_salida',
            'prom_periodo',
            'vector_promedio',
            'semanas',
            'meses',
            'uuid',
            'num_process',
            'num_chunck',
            # 'valor',
            # 'tiempo',
            'columnas',
            'semanas_esp',

        )
        read_only_fields = ('id',)

    def create(self, validated_data):
        columnas_data = validated_data.pop('columnas')
        semanas_esp_data = validated_data.pop('semanas_esp')
        normalizacion = Normalizacion.objects.create(**validated_data)
        for columna_data in columnas_data:
            columna_data['user'] = validated_data['user']
            filtros_data = columna_data.pop('filtros')
            column = Columna.objects.create(normalizacion=normalizacion, **columna_data)
            for filtro in filtros_data:
                filtro['user'] = validated_data['user']
                Filtro.objects.create(columna=column, **filtro)
        for semana_data in semanas_esp_data:
            semana = Semana.objects.create(normalizacion=normalizacion, **semana_data)

        return normalizacion


class NormalizacionPostSerializerWithParams(NormalizacionSerializer):
    api_paramNorm = ApiParaNormPostSerializer(many=False)
    columnas = ColumnaSerializerNoNorm(many=True)

    class Meta:
        model = Normalizacion
        fields = (
            'id',
            'api_paramNorm',
            'nombre_fuente',
            'fecha_inicio',
            'fecha_fin',
            'periodo_salida',
            'prom_periodo',
            'vector_promedio',
            'semanas',
            'meses',
            'valor',
            'columnas'
        )
        read_only_fields = ('id',)

    def create(self, validated_data):

        columnas_data = validated_data.pop('columnas')
        api_aparam = validated_data.pop('api_paramNorm')
        api_catalog = api_aparam.pop('ApiCatalogo')
        api_fuente = api_aparam.pop('Fuente')
        catalogo = ApiCatalogo.objects.create(**api_catalog)
        fuente = TipoFuente.objects.create(**api_fuente)
        api_aparam['ApiCatalogo'] = catalogo
        api_aparam['Fuente'] = fuente
        api_aparam['user'] = validated_data['user']
        param = ParametrosNorm.objects.create(**api_aparam)
        validated_data['api_paramNorm'] = param
        normalizacion = Normalizacion.objects.create(**validated_data)
        for columna_data in columnas_data:
            columna_data['user'] = validated_data['user']
            Columna.objects.create(normalizacion=normalizacion, **columna_data)
        return normalizacion

    def validate(self, data):
        """
        Check that start is before finish.
        """
        if int(data['fecha_inicio']) > int(data['fecha_fin']):
            raise serializers.ValidationError("Finaliza antes de empezar")
        elif int(data['fecha_inicio']) <= 201001:
            raise serializers.ValidationError("La fecha inicial fuera del rango")
        elif int(data['fecha_inicio']) >= 220012:
            raise serializers.ValidationError("La fecha inicial fuera del rango")
        elif int(str(data['fecha_inicio'])[3:]) > 12:
            raise serializers.ValidationError("La meses estan fuera de rango")
        elif int(data['prom_periodo']) > 12:
            raise serializers.ValidationError("La meses estan fuera de rango")

        return data


class NormalizacionDetailSerializer(NormalizacionSerializer):
    """Serialize a ParamBD detail"""
    api_paramNorm = ParamNormSerializer(many=True, read_only=True)


class SemanaSerializer(serializers.ModelSerializer):
    """Serialize a ParamBD"""
    normalizacion = serializers.PrimaryKeyRelatedField(
        many=False,
        queryset=Normalizacion.objects.all()
    )

    class Meta:
        model = Semana
        fields = (
            'id',
            'periodo',
            'semanas',
            'normalizacion'
        )
        read_only_fields = ('id',)
