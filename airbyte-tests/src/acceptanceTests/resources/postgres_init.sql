DROP TABLE IF EXISTS id_and_name;

CREATE
    TABLE
        id_and_name(
            id INTEGER,
            name VARCHAR(200)
        );

INSERT
    INTO
        id_and_name(
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
