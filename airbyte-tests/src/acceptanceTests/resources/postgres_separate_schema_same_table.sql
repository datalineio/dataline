CREATE
    SCHEMA IF NOT EXISTS staging;

DROP TABLE IF EXISTS staging.id_and_name;

CREATE
    TABLE
        staging.id_and_name(
            id INTEGER,
            name VARCHAR(200)
        );

INSERT
    INTO
        staging.id_and_name(
            id,
            name
        )
    VALUES(
        1,
        'sherif'
    ),
    (
        2,
        'charles'
    ),
    (
        3,
        'jared'
    ),
    (
        4,
        'michel'
    ),
    (
        5,
        'john'
    );
