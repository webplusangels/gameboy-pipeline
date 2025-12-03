from unittest.mock import AsyncMock

import pytest

from src.pipeline.batch_processor import BatchProcessor


@pytest.mark.asyncio
async def test_batch_processor_chunks_data_correctly(
    mock_loader: AsyncMock, mock_extractor: AsyncMock
):
    """
    데이터 배치 처리 로직을 테스트합니다.

    Verifies:
        1. 데이터가 올바르게 청크로 나누어지는지
        2. 각 배치가 Loader에 올바르게 전달되는지
    """
    batch_size = 2
    items = [{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}, {"id": 5}]

    async def async_generator(*args, **kwargs):
        for item in items:
            yield item

    mock_extractor.extract.side_effect = async_generator

    processor = BatchProcessor(loader=mock_loader, batch_size=batch_size)

    result = await processor.process(
        extractor=mock_extractor,
        entity_name="test_entity",
        dt_partition="2025-01-01",
    )

    assert result.total_count == len(items)
    assert result.batch_count == 3  # 5개 아이템이므로
    assert len(result.uploaded_files) == 3  # 3개의 배치 파일 업로드

    assert mock_loader.load.call_count == 3


@pytest.mark.asyncio
async def test_batch_processor_handles_no_data(
    mock_loader: AsyncMock, mock_extractor: AsyncMock
):
    """
    데이터가 없는 경우 배치 처리 로직을 테스트합니다.

    Verifies:
        1. 데이터가 없을 때도 올바르게 처리되는지
        2. Loader가 호출되지 않는지
    """

    async def async_generator(*args, **kwargs):
        if False:
            yield  # 아무것도 생성하지 않음

    mock_extractor.extract.side_effect = async_generator

    processor = BatchProcessor(loader=mock_loader, batch_size=2)

    result = await processor.process(
        extractor=mock_extractor,
        entity_name="test_entity",
        dt_partition="2025-01-01",
    )

    assert result.total_count == 0
    assert result.batch_count == 0
    assert len(result.uploaded_files) == 0

    mock_loader.load.assert_not_called()


@pytest.mark.asyncio
async def test_batch_processor_batch_size(
    mock_loader: AsyncMock, mock_extractor: AsyncMock
):
    """
    데이터의 개수와 배치 크기가 일치할 때 불필요한 호출이 없는지 테스트합니다.

    Verifies:
        1. 지정된 배치 크기에 따라 데이터가 청크로 나누어지는지
        2. 불필요한 호출이 없는지
    """
    batch_size = 3
    items = [{"id": 1}, {"id": 2}, {"id": 3}]

    async def async_generator(*args, **kwargs):
        for item in items:
            yield item

    mock_extractor.extract.side_effect = async_generator

    processor = BatchProcessor(loader=mock_loader, batch_size=batch_size)

    result = await processor.process(
        extractor=mock_extractor,
        entity_name="test_entity",
        dt_partition="2025-01-01",
    )

    assert result.total_count == len(items)
    assert result.batch_count == 1  # 정확히 한 배치
    assert len(result.uploaded_files) == 1  # 한 개의 배치 파일 업로드

    assert mock_loader.load.call_count == 1


@pytest.mark.asyncio
async def test_batch_processor_concurrent_mode(
    mock_loader: AsyncMock, mock_extractor: AsyncMock
):
    """
    concurrent=True 옵션으로 병렬 추출을 사용하는지 테스트합니다.

    Verifies:
        1. concurrent=True일 때 extract_concurrent가 호출되는지
        2. 데이터가 올바르게 처리되는지
    """
    batch_size = 2
    items = [{"id": 1}, {"id": 2}, {"id": 3}]

    async def async_generator(*args, **kwargs):
        for item in items:
            yield item

    mock_extractor.extract_concurrent = async_generator

    processor = BatchProcessor(loader=mock_loader, batch_size=batch_size)

    result = await processor.process(
        extractor=mock_extractor,
        entity_name="test_entity",
        dt_partition="2025-01-01",
        concurrent=True,
    )

    # 기존 extract는 호출되지 않아야 함
    mock_extractor.extract.assert_not_called()

    # 결과 검증
    assert result.total_count == len(items)
    assert result.batch_count == 2
    assert mock_loader.load.call_count == 2


@pytest.mark.asyncio
async def test_batch_processor_sequential_mode_default(
    mock_loader: AsyncMock, mock_extractor: AsyncMock
):
    """
    concurrent 옵션 미지정 시 순차 추출(extract)이 기본값인지 테스트합니다.

    Verifies:
        1. concurrent 미지정 시 extract가 호출되는지
        2. extract_concurrent는 호출되지 않는지
    """
    items = [{"id": 1}, {"id": 2}]

    async def async_generator(*args, **kwargs):
        for item in items:
            yield item

    mock_extractor.extract.side_effect = async_generator
    mock_extractor.extract_concurrent = AsyncMock()

    processor = BatchProcessor(loader=mock_loader, batch_size=10)

    await processor.process(
        extractor=mock_extractor,
        entity_name="test_entity",
        dt_partition="2025-01-01",
    )

    mock_extractor.extract.assert_called_once()
    mock_extractor.extract_concurrent.assert_not_called()
