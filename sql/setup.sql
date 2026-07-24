-- En SQL Editor, usa la sintaxis {{parameter}} para parámetros
-- Los parámetros se definen en la UI, no con CREATE WIDGET

CREATE CATALOG IF NOT EXISTS IDENTIFIER('{{catalog}}');

CREATE SCHEMA IF NOT EXISTS IDENTIFIER(CONCAT('{{catalog}}', '.', '{{schema}}'));

CREATE VOLUME IF NOT EXISTS IDENTIFIER(CONCAT('{{catalog}}', '.', '{{schema}}', '.', '{{volume}}'));