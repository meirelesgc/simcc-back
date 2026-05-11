from simcc.repositories import collection_repo, production_repo


async def _filter_collection_entries(admin_session, type, collection_id):
    ids_dict = await collection_repo.get_collection_entries(
        admin_session, type, collection_id
    )
    return ids_dict.get('ids') if ids_dict else None


async def _filter_star_entries(admin_session, type, user):
    ids_dict = await collection_repo.filter_star_entries(
        admin_session, type, user['user_id']
    )
    return ids_dict.get('ids') if ids_dict else None


async def _apply_admin_filters(admin_session, filters, prod_type: str):
    if not admin_session:
        return

    if filters.collection_id:
        filters.collection_id = await _filter_collection_entries(
            admin_session, prod_type, filters.collection_id
        )
    if filters.star:
        filters.star = await _filter_star_entries(
            admin_session, prod_type, filters.star
        )


async def list_papers_magazine(session, admin_session, filters):
    await _apply_admin_filters(admin_session, filters, 'PAPER')
    return await production_repo.list_papers_magazine(session, filters)


async def list_bibliographic_production(
    session, admin_session, filters, qualis=None
):
    await _apply_admin_filters(admin_session, filters, 'ARTICLE')
    return await production_repo.list_bibliographic_production(
        session, filters, qualis
    )


async def list_book_chapter(session, admin_session, filters):
    await _apply_admin_filters(admin_session, filters, 'BOOK_CHAPTER')
    return await production_repo.list_book_chapter(session, filters)


async def list_book(session, admin_session, filters):
    await _apply_admin_filters(admin_session, filters, 'BOOK')
    return await production_repo.list_book(session, filters)
