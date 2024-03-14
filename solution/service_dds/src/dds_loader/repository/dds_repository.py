from datetime import datetime

from lib.pg import PgConnect

class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def h_user_insert(self,
                            user_id: str,
                            load_dt: datetime,
                            load_src: str
                            ) -> None:

        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.h_user(
                        h_user_pk,
                        user_id,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        md5(%(user_id)s::text)::uuid,
                        %(user_id)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT (h_user_pk) DO UPDATE
                    SET
                        user_id = EXCLUDED.user_id,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                    ;
                """,
                    {
                        'user_id': user_id,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )

    def h_product_insert(self,
                            product_id: str,
                            load_dt: datetime,
                            load_src: str
                            ) -> None:

        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.h_product(
                        h_product_pk,
                        product_id,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        md5(%(product_id)s::text)::uuid,
                        %(product_id)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT (h_product_pk) DO UPDATE
                    SET
                        product_id = EXCLUDED.product_id,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                    ;
                """,
                    {
                        'product_id': product_id,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )

    def h_category_insert(self,
                            category_name: str,
                            load_dt: datetime,
                            load_src: str
                            ) -> None:

        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.h_category(
                        h_category_pk,
                        category_name,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        md5(%(category_name)s::text)::uuid,
                        %(category_name)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT (h_category_pk) DO UPDATE
                    SET
                        category_name = EXCLUDED.category_name,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                    ;
                """,
                    {
                        'category_name': category_name,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )

    def h_restaurant_insert(self,
                            restaurant_id: str,
                            load_dt: datetime,
                            load_src: str
                            ) -> None:

        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.h_restaurant(
                        h_restaurant_pk,
                        restaurant_id,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        md5(%(restaurant_id)s::text)::uuid,
                        %(restaurant_id)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT (h_restaurant_pk) DO UPDATE
                    SET
                        restaurant_id = EXCLUDED.restaurant_id,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                    ;
                """,
                    {
                        'restaurant_id': restaurant_id,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )

    def h_order_insert(self,
                            order_id: str,
                            order_dt: datetime,
                            load_dt: datetime,
                            load_src: str
                            ) -> None:

        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.h_order(
                        h_order_pk,
                        order_id,
                        order_dt,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        md5(%(order_id)s::text)::uuid,
                        %(order_id)s,
                        %(order_dt)s,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT (h_order_pk) DO UPDATE
                    SET
                        order_id = EXCLUDED.order_id,
                        order_dt = EXCLUDED.order_dt,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                    ;
                """,
                    {
                        'order_id': order_id,
                        'order_dt': order_dt,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )

    def l_order_product_insert(self,
                                order_id: str,
                                product_id: str,
                                load_dt: datetime,
                                load_src: str
                                ) -> None:

        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.l_order_product(
                        hk_order_product_pk,
                        h_order_pk,
                        h_product_pk,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        md5(%(order_id)s::text || %(product_id)s::text)::uuid,
                        md5(%(order_id)s::text)::uuid,
                        md5(%(product_id)s::text)::uuid,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT (hk_order_product_pk) DO UPDATE
                    SET
                        h_order_pk = EXCLUDED.h_order_pk,
                        h_product_pk = EXCLUDED.h_product_pk,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                    ;
                """,
                    {
                        'order_id': order_id,
                        'product_id': product_id,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )


    def l_product_restaurant_insert(self,
                                    product_id: str,
                                    restaurant_id: str,
                                    load_dt: datetime,
                                    load_src: str
                                    ) -> None:

        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.l_product_restaurant(
                        hk_product_restaurant_pk,
                        h_product_pk,
                        h_restaurant_pk,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        md5(%(product_id)s::text || %(restaurant_id)s::text)::uuid,
                        md5(%(product_id)s::text)::uuid,
                        md5(%(restaurant_id)s::text)::uuid,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT (hk_product_restaurant_pk) DO UPDATE
                    SET
                        h_product_pk = EXCLUDED.h_product_pk,
                        h_restaurant_pk = EXCLUDED.h_restaurant_pk,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                    ;
                """,
                    {
                        'product_id': product_id,
                        'restaurant_id': restaurant_id,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )


    def l_product_category_insert(self,
                                    product_id: str,
                                    category_name: str,
                                    load_dt: datetime,
                                    load_src: str
                                    ) -> None:

        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.l_product_category(
                        hk_product_category_pk,
                        h_product_pk,
                        h_category_pk,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        md5(%(product_id)s::text || %(category_name)s::text)::uuid,
                        md5(%(category_name)s::text)::uuid,
                        md5(%(category_name)s::text)::uuid,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT (hk_product_category_pk) DO UPDATE
                    SET
                        h_product_pk = EXCLUDED.h_product_pk,
                        h_category_pk = EXCLUDED.h_category_pk,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                    ;
                """,
                    {
                        'product_id': product_id,
                        'category_name': category_name,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )


    def l_order_user_insert(self,
                                order_id: str,
                                user_id: str,
                                load_dt: datetime,
                                load_src: str
                                ) -> None:

        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.l_order_user(
                        hk_order_user_pk,
                        h_order_pk,
                        h_user_pk,
                        load_dt,
                        load_src
                    )
                    VALUES(
                        md5(%(order_id)s::text || %(user_id)s::text)::uuid,
                        md5(%(order_id)s::text)::uuid,
                        md5(%(user_id)s::text)::uuid,
                        %(load_dt)s,
                        %(load_src)s
                    )
                    ON CONFLICT (hk_order_user_pk) DO UPDATE
                    SET
                        h_order_pk = EXCLUDED.h_order_pk,
                        h_user_pk = EXCLUDED.h_user_pk,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src
                    ;
                """,
                    {
                        'order_id': order_id,
                        'user_id': user_id,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )

    def s_user_names_insert(self,
                                user_id: str,
                                username: str,
                                userlogin: str,
                                load_dt: datetime,
                                load_src: str
                                ) -> None:

        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.s_user_names(
                        h_user_pk,
                        username,
                        userlogin,
                        load_dt,
                        load_src,
                        hk_user_names_hashdiff
                    )
                    VALUES(
                        md5(%(user_id)s::text)::uuid,
                        %(username)s,
                        %(userlogin)s,
                        %(load_dt)s,
                        %(load_src)s,
                        md5(md5(%(user_id)s::text)::uuid::text || %(username)s::text || %(userlogin)s::text || %(load_dt)s::text || %(load_src)s::text)::uuid
                    )
                    ON CONFLICT (h_user_pk, load_dt) DO UPDATE
                    SET
                        username = EXCLUDED.username,
                        userlogin = EXCLUDED.userlogin,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src,
                        hk_user_names_hashdiff = EXCLUDED.hk_user_names_hashdiff
                    ;
                """,
                    {
                        'user_id': user_id,
                        'username': username,
                        'userlogin': userlogin,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )


    def s_product_names_insert(self,
                                product_id: str,
                                product_name: str,
                                load_dt: datetime,
                                load_src: str
                                ) -> None:

        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.s_product_names(
                        h_product_pk,
                        name,
                        load_dt,
                        load_src,
                        hk_product_names_hashdiff
                    )
                    VALUES(
                        md5(%(product_id)s::text)::uuid,
                        %(product_name)s,
                        %(load_dt)s,
                        %(load_src)s,
                        md5(md5(%(product_id)s::text)::uuid::text || %(product_name)s::text || %(load_dt)s::text || %(load_src)s::text)::uuid
                    )
                    ON CONFLICT (h_product_pk, load_dt) DO UPDATE
                    SET
                        name = EXCLUDED.name,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src,
                        hk_product_names_hashdiff = EXCLUDED.hk_product_names_hashdiff
                    ;
                """,
                    {
                        'product_id': product_id,
                        'product_name': product_name,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )


    def s_restaurant_names_insert(self,
                                    restaurant_id: str,
                                    restaurant_name: str,
                                    load_dt: datetime,
                                    load_src: str
                                    ) -> None:

        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.s_restaurant_names(
                        h_restaurant_pk,
                        name,
                        load_dt,
                        load_src,
                        hk_restaurant_names_hashdiff
                    )
                    VALUES(
                        md5(%(restaurant_id)s::text)::uuid,
                        %(restaurant_name)s,
                        %(load_dt)s,
                        %(load_src)s,
                        md5(md5(%(restaurant_id)s::text)::uuid::text || %(restaurant_name)s::text || %(load_dt)s::text || %(load_src)s::text)::uuid
                    )
                    ON CONFLICT (h_restaurant_pk, load_dt) DO UPDATE
                    SET
                        name = EXCLUDED.name,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src,
                        hk_restaurant_names_hashdiff = EXCLUDED.hk_restaurant_names_hashdiff
                    ;
                """,
                    {
                        'restaurant_id': restaurant_id,
                        'restaurant_name': restaurant_name,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )

    def s_order_cost_insert(self,
                                order_id: str,
                                order_cost: str,
                                order_payment: str,
                                load_dt: datetime,
                                load_src: str
                                ) -> None:

        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.s_order_cost(
                        h_order_pk,
                        cost,
                        payment,
                        load_dt,
                        load_src,
                        hk_order_cost_hashdiff
                    )
                    VALUES(
                        md5(%(order_id)s::text)::uuid,
                        %(order_cost)s,
                        %(order_payment)s,
                        %(load_dt)s,
                        %(load_src)s,
                        md5(md5(%(order_id)s::text)::uuid::text || %(order_cost)s::text || %(order_payment)s::text || %(load_dt)s::text || %(load_src)s::text)::uuid
                    )
                    ON CONFLICT (h_order_pk, load_dt) DO UPDATE
                    SET
                        cost = EXCLUDED.cost,
                        payment = EXCLUDED.payment,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src,
                        hk_order_cost_hashdiff = EXCLUDED.hk_order_cost_hashdiff
                    ;
                """,
                    {
                        'order_id': order_id,
                        'order_cost': order_cost,
                        'order_payment': order_payment,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )

    def s_order_status_insert(self,
                                order_id: str,
                                order_status: str,
                                load_dt: datetime,
                                load_src: str
                                ) -> None:

        with self._db.pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                """
                    INSERT INTO dds.s_order_status(
                        h_order_pk,
                        status,
                        load_dt,
                        load_src,
                        hk_order_status_hashdiff
                    )
                    VALUES(
                        md5(%(order_id)s::text)::uuid,
                        %(order_status)s,
                        %(load_dt)s,
                        %(load_src)s,
                        md5(md5(%(order_id)s::text)::uuid::text || %(order_status)s::text || %(load_dt)s::text || %(load_src)s::text)::uuid
                    )
                    ON CONFLICT (h_order_pk, load_dt) DO UPDATE
                    SET
                        status = EXCLUDED.status,
                        load_dt = EXCLUDED.load_dt,
                        load_src = EXCLUDED.load_src,
                        hk_order_status_hashdiff = EXCLUDED.hk_order_status_hashdiff
                    ;
                """,
                    {
                        'order_id': order_id,
                        'order_status': order_status,
                        'load_dt': load_dt,
                        'load_src': load_src
                    }
                )