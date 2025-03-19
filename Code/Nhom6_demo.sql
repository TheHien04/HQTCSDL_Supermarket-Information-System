USE HQTCSDL
GO

--------------------------------------- TRƯỜNG HỢP 1 LOST UPDATE----------------------------------------
--Tạo proc có delay
CREATE OR ALTER PROCEDURE usp_CapNhatSL_HT_1
    @IDSanPham VARCHAR(15),
    @SoLuong INT
AS
BEGIN
    BEGIN TRANSACTION
    --SET TRANSACTION ISOLATION LEVEL SERIALIZABLE

    DECLARE @SLHT INT
    SELECT @SLHT = SL_HT
    FROM KhoHang 
	-- WITH (ROWLOCK, HOLDLOCK)
    WHERE IDSanPham = @IDSanPham

    -- Giảm số lượng hàng tồn kho
    SET @SLHT -= @SoLuong

    -- Giả lập thời gian chờ để tạo tranh chấp
    WAITFOR DELAY '00:00:10'; -- Chờ 10 giây

    UPDATE KhoHang 
	-- WITH (ROWLOCK, HOLDLOCK)
    SET SL_HT = @SLHT
    WHERE IDSanPham = @IDSanPham

    COMMIT TRANSACTION
END

 --chạy trong query 1
EXEC usp_CapNhatSL_HT_1 @IDSanPham = 'SP001', @SoLuong = 10;

-- chạy trong query khác
EXEC usp_CapNhatSL_HT @IDSanPham = 'SP001', @SoLuong = 20;

-- khi chạy sẽ chạy query 2 trước set data còn 30 nhưng query 1 sau 10s sẽ commit set data thành 40 trong kho hàng gây lost update.


--------------------------------------- TRƯỜNG HỢP 2 LOST UPDATE----------------------------------------
--Tạo proc có delay
GO
CREATE OR ALTER PROC usp_DieuChinhPhanHangKhachHang_1
    @SDT VARCHAR(15) 
AS
BEGIN
    BEGIN TRANSACTION; 
    -- SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; 

    BEGIN TRY
        -- 1. Kiểm tra tính hợp lệ của số điện thoại
        IF NOT EXISTS (
            SELECT 1 
            FROM KHACHHANG 
			-- WITH (READCOMMITTED) 
            WHERE SDT = @SDT
        )
        BEGIN
            RAISERROR(N'Số điện thoại không tồn tại trong hệ thống.', 16, 1);
            ROLLBACK TRANSACTION;
            RETURN;
        END

        -- 2. Tính tổng chi tiêu năm trước của khách hàng
        DECLARE @TongChiTieu BIGINT;
        SELECT @TongChiTieu = SUM(TongGiaTriDonHang)
        FROM DonHang 
		-- WITH (REPEATABLEREAD) -- Giữ shared lock trong suốt giao dịch
        WHERE SDT = @SDT 
          AND NgayBan >= DATEADD(YEAR, -1, GETDATE()) 
          AND TinhTrangDonHang = N'Thành công';

		WAITFOR DELAY '00:00:10'; -- Chờ 10 giây

        -- 3. Điều chỉnh phân hạng dựa trên tổng chi tiêu
        DECLARE @LoaiThe NVARCHAR(50);
        IF @TongChiTieu >= 50000000
            SET @LoaiThe = N'Kim Cương';
        ELSE IF @TongChiTieu >= 30000000
            SET @LoaiThe = N'Bạch Kim';
        ELSE IF @TongChiTieu >= 15000000
            SET @LoaiThe = N'Vàng';
        ELSE IF @TongChiTieu >= 5000000
            SET @LoaiThe = N'Bạc';
        ELSE IF @TongChiTieu >= 1000000
            SET @LoaiThe = N'Đồng';
        ELSE
            SET @LoaiThe = N'Thân Thiết';
        -- 4. Cập nhật loại thẻ của khách hàng trong bảng KHACHHANGTHE
        UPDATE KHACHHANGTHE 
		-- WITH (HOLDLOCK, ROWLOCK) -- Giữ shared lock trên dòng
        SET LoaiThe = @LoaiThe
        WHERE SDT = @SDT;

        COMMIT TRANSACTION;
        PRINT N'Cập nhật phân hạng khách hàng thành công.';
    END TRY
    BEGIN CATCH
       
        ROLLBACK TRANSACTION;
        PRINT N'Lỗi trong quá trình cập nhật phân hạng khách hàng: ' + ERROR_MESSAGE();
    END CATCH
END
GO

-- chạy trong query 1
EXEC usp_DieuChinhPhanHangKhachHang_1 '0934567890'

-- chạy trong query khác
EXEC usp_TraLaiDonHang 'DH005'
EXEC usp_DieuChinhPhanHangKhachHang '0934567890' -- Là proc không có delay trong code của usp_DieuChinhPhanHangKhachHang_1

-- khi chạy, query 2 sẽ chạy trước sẽ trả lại đơn hàng, rồi sẽ update hạng (Thân thiết) theo Tổng chi tiêu không có đơn hàng do đơn hàng đã bị trả lại 
-- nhưng query 1 sau 10s sẽ commit sẽ vẫn update hạng thành viên theo số tiền cũ (Bạc) gây lost update, 
-- với update bị lost là số tiền tiêu dùng 1 năm của khách.

--------------------------------------- TRƯỜNG HỢP 3 LOST UPDATE----------------------------------------
--Tạo proc có delay
GO
CREATE OR ALTER PROCEDURE usp_ThemSanPhamDonHang_1
	@MaDonHang VARCHAR(15),
	@IDSanPham VARCHAR(15),
	@Soluong INT
AS
BEGIN
	BEGIN TRY
	BEGIN TRANSACTION 
	-- SET TRANSACTION ISOLATION LEVEL SERIALIZABLE

	IF EXISTS (SELECT 1 FROM SanPhamDonHang WHERE IDSanPham = @IDSanPham AND MaDonHang = @MaDonHang)
	BEGIN
		RAISERROR(N'Đã tồn tại đơn hàng có sản phẩm đó', 16,1)
		RETURN
	END

	INSERT INTO SanPhamDonHang 
	-- WITH (TABLOCK) 
	VALUES (@MaDonHang, @IDSanPham, @Soluong)

	WAITFOR DELAY '00:00:10'

	UPDATE DonHang 
	-- WITH (UPDLOCK)
	SET TongGiaTriDonHang += (SELECT @Soluong * s.GiaNiemYet FROM SANPHAM s  WHERE s.IDSanPham = @IDSanPham)
	WHERE MaDonHang = @MaDonHang

	COMMIT TRANSACTION

	END TRY
	BEGIN CATCH
	PRINT(N'Lỗi:' + ERROR_MESSAGE());
	ROLLBACK TRANSACTION
	END CATCH
END
GO

-- chạy trong query 1
EXEC usp_ThemSanPhamDonHang_1 'DH001', 'SP006', 1

-- chạy trong query khác 
EXEC usp_XoaSanPhamDonHang 'DH001', 'SP001'

-- giá trị bị cập nhật sai, lost update trên tổng giá trị đơn hàng của đơn hàng


--------------------------------------- TRƯỜNG HỢP 1 PHANTOM READ ----------------------------------------
-- Tạo proc có delay
CREATE OR ALTER PROCEDURE usp_GuiPhieuMuaHangSinhNhat_1
    @ThangSinh INT 
AS
BEGIN
    BEGIN TRANSACTION;
    -- SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;

    BEGIN TRY
        -- 1. Truy vấn danh sách khách hàng có sinh nhật trong tháng
        DECLARE @DanhSachKhachHang TABLE (
            SDT VARCHAR(15),
            LoaiThe NVARCHAR(50)
        );

        INSERT INTO @DanhSachKhachHang (SDT, LoaiThe)
        SELECT KH.SDT, KHT.LoaiThe
        FROM KHACHHANG KH 
        INNER JOIN KHACHHANGTHE KHT ON KH.SDT = KHT.SDT
        WHERE MONTH(KHT.NgaySinh) = @ThangSinh;

        WAITFOR DELAY '00:00:10'; -- Chờ 10 giây để tạo tranh chấp

        -- 2. Gửi phiếu giảm giá cho từng khách hàng trong danh sách
        DECLARE @SDT VARCHAR(15), @LoaiThe NVARCHAR(50), @MaPhieu VARCHAR(15), @TienGiamGia INT;
        DECLARE KhachHangCursor CURSOR FOR SELECT SDT, LoaiThe FROM @DanhSachKhachHang;
        OPEN KhachHangCursor;

        FETCH NEXT FROM KhachHangCursor INTO @SDT, @LoaiThe;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            -- Tính tiền giảm giá dựa trên loại thẻ
            IF @LoaiThe = N'Kim Cương'
                SET @TienGiamGia = 1200000;
            ELSE IF @LoaiThe = N'Bạch Kim'
                SET @TienGiamGia = 700000;
            ELSE IF @LoaiThe = N'Vàng'
                SET @TienGiamGia = 500000;
            ELSE IF @LoaiThe = N'Bạc'
                SET @TienGiamGia = 200000;
            ELSE IF @LoaiThe = N'Đồng'
                SET @TienGiamGia = 100000;
            ELSE
                SET @TienGiamGia = 50000;

            -- Sinh mã phiếu giảm giá
            SET @MaPhieu = 'PGG' + RIGHT(CAST(ABS(CHECKSUM(NEWID())) AS VARCHAR), 6);

            -- Gửi phiếu giảm giá vào bảng PHIEUGIAMGIA
            INSERT INTO PHIEUGIAMGIA (SDT, MaPhieu, TienGiamGia, NgayBatDau, NgayKetThuc)
            VALUES (@SDT, @MaPhieu, @TienGiamGia, GETDATE(), DATEADD(MONTH, 1, GETDATE()));

            FETCH NEXT FROM KhachHangCursor INTO @SDT, @LoaiThe;
        END;

        CLOSE KhachHangCursor;
        DEALLOCATE KhachHangCursor;

        COMMIT TRANSACTION;
        PRINT N'Gửi phiếu giảm giá sinh nhật thành công.';
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        PRINT N'Lỗi: ' + ERROR_MESSAGE();
    END CATCH
END
GO

-- Chạy trong query 1
EXEC usp_GuiPhieuMuaHangSinhNhat_1 @ThangSinh = 12;

-- Chạy trong query khác
EXEC usp_QuanLyTaiKhoanKhachHang 
    @SDT = '0999999999', 
    @Ten = N'Nguyễn Văn Phantom', 
    @LoaiThe = N'Vàng', 
    @NgayLapThe = '2023-12-15', 
    @Action = 'C';

--------------------------------------- TRƯỜNG HỢP 2 PHANTOM READ ----------------------------------------
-- Tạo proc có delay
CREATE OR ALTER PROCEDURE usp_CapNhatPhanHangDauThang_1
AS
BEGIN
    BEGIN TRANSACTION;
    -- SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;

    BEGIN TRY
        -- 1. Truy vấn danh sách tất cả khách hàng và tính tổng chi tiêu
        DECLARE @DanhSachKhachHang TABLE (
            SDT VARCHAR(15),
            TongChiTieu BIGINT,
            LoaiThe NVARCHAR(50)
        );

        INSERT INTO @DanhSachKhachHang (SDT, TongChiTieu, LoaiThe)
        SELECT KH.SDT, ISNULL(SUM(DH.TongGiaTriDonHang), 0), ''
        FROM KHACHHANG KH 
        LEFT JOIN DonHang DH ON KH.SDT = DH.SDT AND DH.NgayBan >= DATEADD(YEAR, -1, GETDATE())
        GROUP BY KH.SDT;

        WAITFOR DELAY '00:00:10'; -- Chờ 10 giây để tạo tranh chấp

        -- 2. Cập nhật loại thẻ cho từng khách hàng trong danh sách
        UPDATE @DanhSachKhachHang 
        SET LoaiThe = CASE
            WHEN TongChiTieu >= 50000000 THEN N'Kim Cương'
            WHEN TongChiTieu >= 30000000 THEN N'Bạch Kim'
            WHEN TongChiTieu >= 15000000 THEN N'Vàng'
            WHEN TongChiTieu >= 5000000 THEN N'Bạc'
            WHEN TongChiTieu >= 1000000 THEN N'Đồng'
            ELSE N'Thân Thiết'
        END;

        -- 3. Áp dụng cập nhật vào bảng KHACHHANGTHE
        UPDATE KHACHHANGTHE
        SET LoaiThe = DS.LoaiThe
        FROM KHACHHANGTHE KHT
        INNER JOIN @DanhSachKhachHang DS ON KHT.SDT = DS.SDT;

        COMMIT TRANSACTION;
        PRINT N'Cập nhật phân hạng khách hàng thành công.';
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        PRINT N'Lỗi: ' + ERROR_MESSAGE();
    END CATCH
END
GO

-- Chạy trong query 1
EXEC usp_CapNhatPhanHangDauThang_1;

-- Chạy trong query khác
EXEC usp_QuanLyTaiKhoanKhachHang 
    @SDT = '0988888888', 
    @Ten = N'Trần Văn Phantom', 
    @LoaiThe = N'Bạc', 
    @NgayLapThe = '2023-12-01', 
    @Action = 'C';

INSERT INTO DonHang (MaDonHang, SDT, NgayBan, TongGiaTriDonHang, CachThanhToan, TinhTrangDonHang)
VALUES ('DH999', '0988888888', '2023-12-15', 60000000, N'Tiền mặt', N'Thành công');


-- ========= TÌNH HUỐNG 3 PHANTOM READ ========
go
create or alter proc usp_KhachHangDoanhThu_notHandled
	@Ngay Date
as
begin 
	begin tran
		--set tran isolation level serializable
		if not exists (
						select 1
						from DonHang 
						--with (rowlock, holdlock)
						where NgayBan = @Ngay
					  )
			begin
				;throw 50000, N'Ngày này không có người mua hàng', 1
				return
			end

		-- Cho bộ phận A
		select count(*) as N'Tổng lượng khách hàng', sum(TongGiaTriDonHang) as N'Tổng doanh thu'
		from DonHang 
		--with (rowlock, holdlock)
		where NgayBan = @Ngay and TinhTrangDonHang <> N'Trả lại'
		group by NgayBan

		--waitfor delay '00:00:10'

		---- Cho bộ phận B
		--select count(*) as N'Tổng lượng khách hàng', sum(TongGiaTriDonHang) as N'Tổng doanh thu'
		--from DonHang 
		----with (rowlock, holdlock)
		--where NgayBan = @Ngay and TinhTrangDonHang <> N'Trả lại'
		--group by NgayBan

	commit tran
end
go

CREATE OR ALTER PROCEDURE usp_TraLaiDonHang
	@MaDonHang VARCHAR(15)
AS
BEGIN
	BEGIN TRY

	BEGIN TRAN
	
	SET TRANSACTION ISOLATION LEVEL SERIALIZABLE

	IF NOT EXISTS (SELECT 1 FROM DonHang WITH (ROWLOCK, HOLDLOCK) WHERE MaDonHang = @MaDonHang)
	BEGIN
		RAISERROR(N'Không tồn tại đơn hàng', 16,1);
		RETURN;
	END

	UPDATE DonHang WITH (UPDLOCK)
	SET TinhTrangDonHang = N'Trả lại'
	WHERE MaDonHang = @MaDonHang

	DECLARE @Chitietdonhang ChiTietDonHang;

	INSERT INTO @Chitietdonhang
	SELECT IDSanPham, SoLuong 
	FROM SanPhamDonHang 
	WITH (ROWLOCK, HOLDLOCK)
	WHERE MaDonHang = @MaDonHang

	DECLARE cur CURSOR FOR
	SELECT IDSanPham, Quantity FROM @Chitietdonhang;
	
	DECLARE @IDSanPham VARCHAR(15);
	DECLARE @SoLuong INT;

	OPEN cur;
    FETCH NEXT FROM cur INTO @IDSanPham, @SoLuong;

	WHILE @@FETCH_STATUS = 0
        BEGIN
			BEGIN TRANSACTION;
				UPDATE KhoHang WITH (ROWLOCK, HOLDLOCK)
				SET SL_HT += @SoLuong
				WHERE IDSanPham = @IDSanPham
			FETCH NEXT FROM cur INTO @IDSanPham, @SoLuong;
			COMMIT TRANSACTION;
		END
	END TRY
	BEGIN CATCH
		ROLLBACK TRANSACTION;
		PRINT(N'Lỗi:' + ERROR_MESSAGE());
	END CATCH
	COMMIT TRAN
END
go

-- chạy trong query 1
exec dbo.usp_KhachHangDoanhThu_notHandled '2024-12-10'
go


-- chạy trong 1 query khác
EXEC dbo.usp_TraLaiDonHang 'DH010'
GO


-- ============= TÌNH HUỐNG 2 REPEATABLE READ ================
go
create or alter proc usp_ThongKeTraHang_notHandled
	@MaDh varchar(15)
as
begin
	begin tran 
	--set tran isolation level repeatable read
		select spdh.IDSanPham as N'Sản phẩm', sum(spdh.SoLuong) as N'Số lượng'
		from DonHang dh 
		--with (repeatableread) 
		join SanPhamDonHang spdh 
		--with (repeatableread)
		on dh.MaDonHang = spdh.MaDonHang
		where @MaDh = dh.MaDonHang and dh.TinhTrangDonHang = N'Trả lại'
		group by spdh.IDSanPham

		waitfor delay '00:00:10'

		select spdh.IDSanPham as N'Sản phẩm', sum(spdh.SoLuong) as N'Số lượng bị trả'
		from DonHang dh 
		--with (repeatableread)
		join SanPhamDonHang spdh
		--with (repeatableread)
		on dh.MaDonHang = spdh.MaDonHang
		where  @MaDh = dh.MaDonHang and dh.TinhTrangDonHang = N'Trả lại'
		group by spdh.IDSanPham

	commit tran
end
GO

-- chạy trong query 1
exec dbo.usp_ThongKeTraHang_notHandled 'DH009'
go

-- chạy trong 1 query window khác
exec dbo.usp_UpdateSanPhamDonHang 'DH009', 'SP018', 3
go