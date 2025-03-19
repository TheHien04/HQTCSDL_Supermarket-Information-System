USE HQTCSDL
GO

----------------------------------------------------BỘ PHẬN CHĂM SÓC KHÁCH HÀNG---------------------------------
CREATE OR ALTER PROC usp_DieuChinhPhanHangKhachHang
    @SDT VARCHAR(15) 
AS
BEGIN
    BEGIN TRANSACTION; 
    SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; 

    BEGIN TRY
        -- 1. Kiểm tra tính hợp lệ của số điện thoại
        IF NOT EXISTS (
            SELECT 1 
            FROM KHACHHANG WITH (READCOMMITTED) 
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
        FROM DonHang WITH (REPEATABLEREAD) -- Giữ shared lock trong suốt giao dịch
        WHERE SDT = @SDT 
          AND NgayBan >= DATEADD(YEAR, -1, GETDATE()) 
          AND TinhTrangDonHang = N'Thành công';

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
        UPDATE KHACHHANGTHE WITH (HOLDLOCK, ROWLOCK) -- Giữ shared lock trên dòng
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


GO
CREATE OR ALTER PROC usp_CapNhatPhanHangDauThang
AS
BEGIN
	BEGIN TRANSACTION; 
	SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;

	BEGIN TRY
		-- 1. Truy vấn danh sách tất cả khách hàng
		DECLARE @DanhSachKhachHang TABLE (
			SDT VARCHAR(15),
			TongChiTieu BIGINT,
			LoaiThe NVARCHAR(50)
		);

		INSERT INTO @DanhSachKhachHang (SDT, TongChiTieu, LoaiThe)
		SELECT
			KH.SDT,
			ISNULL(SUM(DH.TongGiaTriDonHang), 0) AS TongChiTieu,
			'' AS LoaiThe
		FROM
			KHACHHANG KH WITH (READCOMMITTED) -- Đảm bảo đọc dữ liệu đã commit
		LEFT JOIN
			DonHang DH WITH (REPEATABLEREAD) -- Giữ shared lock trên các dòng trong suốt giao dịch
		ON
			KH.SDT = DH.SDT
			AND DH.NgayBan >= DATEADD(YEAR, -1, GETDATE())
			AND DH.TinhTrangDonHang = N'Thành công'
		GROUP BY
			KH.SDT;

		-- 2. Cập nhật loại thẻ dựa trên tổng chi tiêu
		UPDATE @DanhSachKhachHang 
		SET LoaiThe = CASE
			WHEN TongChiTieu >= 50000000 THEN N'Kim Cương'
			WHEN TongChiTieu >= 30000000 THEN N'Bạch Kim'
			WHEN TongChiTieu >= 15000000 THEN N'Vàng'
			WHEN TongChiTieu >= 5000000 THEN N'Bạc'
			WHEN TongChiTieu >= 1000000 THEN N'Đồng'
			ELSE N'Thân Thiết'
		END;

		-- 3. Áp dụng cập nhật phân hạng khách hàng vào bảng KHACHHANGTHE
		UPDATE KHACHHANGTHE WITH (HOLDLOCK, ROWLOCK) -- Giữ shared lock và giới hạn phạm vi khóa
		SET LoaiThe = DS.LoaiThe
		FROM KHACHHANGTHE KHT
		INNER JOIN @DanhSachKhachHang DS
		ON KHT.SDT = DS.SDT;

		COMMIT TRANSACTION;
		PRINT N'Cập nhật phân hạng khách hàng đầu tháng thành công.';
	END TRY
	BEGIN CATCH
		
		ROLLBACK TRANSACTION;
		PRINT N'Lỗi trong quá trình cập nhật phân hạng khách hàng: ' + ERROR_MESSAGE();
	END CATCH
END 
GO


GO
CREATE OR ALTER PROC usp_QuanLyTaiKhoanKhachHang
	@SDT VARCHAR(15),
	@Ten NVARCHAR(50),
	@LoaiThe NVARCHAR(30),
	@NgayLapThe DATE,
	@Action CHAR(1)
AS
BEGIN
	BEGIN TRANSACTION;
	SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;

	BEGIN TRY
		-- Kiểm tra hành động
		IF @Action NOT IN ('C', 'U', 'D')
		BEGIN
			RAISERROR(N'Hành động không hợp lệ. Chỉ được phép: C (Thêm), U (Sửa), D (Xóa).', 16, 1);
			ROLLBACK TRANSACTION;
			RETURN;
		END;

		-- Thêm tài khoản khách hàng
		IF @Action = 'C'
		BEGIN
			IF EXISTS (SELECT 1 FROM KHACHHANG WITH (UPDLOCK, HOLDLOCK) WHERE SDT = @SDT)
			BEGIN
				RAISERROR(N'Số điện thoại đã tồn tại. Không thể thêm mới.', 16, 1);
				ROLLBACK TRANSACTION;
				RETURN;
			END

			INSERT INTO KHACHHANG WITH (HOLDLOCK, ROWLOCK)
			VALUES (@SDT, @Ten);

			INSERT INTO KHACHHANGTHE WITH (HOLDLOCK, ROWLOCK)
			VALUES (@SDT, NULL, @NgayLapThe, @LoaiThe);

			PRINT N'Thêm tài khoản khách hàng thành công.';
		END

		-- Sửa thông tin khách hàng
		ELSE IF @Action = 'U'
		BEGIN
			IF NOT EXISTS (SELECT 1 FROM KHACHHANG WITH (UPDLOCK, HOLDLOCK) WHERE SDT = @SDT)
			BEGIN
				RAISERROR(N'Số điện thoại không tồn tại. Không thể sửa.', 16, 1);
				ROLLBACK TRANSACTION;
				RETURN;
			END

			UPDATE KHACHHANG WITH (ROWLOCK)
			SET Ten = @Ten
			WHERE SDT = @SDT;

			UPDATE KHACHHANGTHE WITH (ROWLOCK)
			SET NgayLapThe = @NgayLapThe,
				LoaiThe = @LoaiThe
			WHERE SDT = @SDT;

			PRINT N'Cập nhật thông tin khách hàng thành công.';
		END

		-- Xóa tài khoản khách hàng
		ELSE IF @Action = 'D'
		BEGIN
			IF NOT EXISTS (SELECT 1 FROM KHACHHANG WITH (UPDLOCK, HOLDLOCK) WHERE SDT = @SDT)
			BEGIN
				RAISERROR(N'Số điện thoại không tồn tại. Không thể xóa.', 16, 1);
				ROLLBACK TRANSACTION;
				RETURN;
			END;

			DELETE FROM KHACHHANG WITH (ROWLOCK) WHERE SDT = @SDT;

			DELETE FROM KHACHHANGTHE WITH (ROWLOCK) WHERE SDT = @SDT;

			PRINT N'Xóa tài khoản khách hàng thành công.';
		END

		COMMIT TRANSACTION;
	END TRY
	BEGIN CATCH
		ROLLBACK TRANSACTION;
		PRINT N'Lỗi trong quá trình quản lý tài khoản khách hàng: ' + ERROR_MESSAGE();
	END CATCH
END
GO

CREATE OR ALTER PROC usp_Gui1PhieuMuaHang
	@SDT VARCHAR(15),
	@LoaiThe NVARCHAR(20),
	@MaPhieu VARCHAR(15) OUTPUT,
	@TienGiamGia INT OUTPUT,
	@NgayBatDau DATE OUTPUT,
	@NgayKetThuc DATE OUTPUT
AS
BEGIN
	BEGIN TRANSACTION;
	SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;

	BEGIN TRY
		-- 1. Kiểm tra số điện thoại có tồn tại hay không
		IF NOT EXISTS (SELECT 1 FROM KHACHHANG WITH (UPDLOCK, HOLDLOCK) WHERE SDT = @SDT)
		BEGIN
			RAISERROR(N'Số điện thoại không tồn tại.', 16, 1);
			ROLLBACK TRANSACTION;
			RETURN;
		END;

		-- 2. Tính giá trị tiền giảm giá dựa trên loại thẻ 
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

        -- 3. Tạo mã phiếu giảm giá ngẫu nhiên
        SET @MaPhieu = 'PGG' + RIGHT(CAST(ABS(CHECKSUM(NEWID())) AS VARCHAR), 6);

        -- 4. Xác định ngày bắt đầu và ngày kết thúc
        SET @NgayBatDau = GETDATE();
        SET @NgayKetThuc = DATEADD(MONTH, 1, @NgayBatDau);

        -- 5. Thêm phiếu giảm giá vào bảng PHIEUGIAMGIA
        INSERT INTO PHIEUGIAMGIA WITH (HOLDLOCK, ROWLOCK)
        VALUES (@SDT, @MaPhieu, @TienGiamGia, @NgayBatDau, @NgayKetThuc);

        COMMIT TRANSACTION;
        PRINT N'Gửi phiếu giảm giá thành công.';
    END TRY
    BEGIN CATCH
        
        ROLLBACK TRANSACTION;
        PRINT N'Lỗi trong quá trình gửi phiếu giảm giá: ' + ERROR_MESSAGE();
    END CATCH
END
GO

CREATE OR ALTER PROC usp_GuiPhieuMuaHangSinhNhat
    @ThangSinh INT 
AS
BEGIN 
    SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; 

    BEGIN TRY
        -- 1. Truy vấn danh sách khách hàng có sinh nhật trong tháng
        DECLARE @DanhSachKhachHang TABLE (
            SDT VARCHAR(15),
            LoaiThe NVARCHAR(50)
        );

        INSERT INTO @DanhSachKhachHang (SDT, LoaiThe)
        SELECT KH.SDT, KHT.LoaiThe
        FROM KHACHHANG KH WITH (UPDLOCK, HOLDLOCK)
        INNER JOIN KHACHHANGTHE KHT WITH (UPDLOCK, HOLDLOCK) ON KH.SDT = KHT.SDT
        WHERE MONTH(KHT.NgaySinh) = @ThangSinh;

        -- 2. Gửi phiếu giảm giá cho từng khách hàng
        DECLARE @SDT VARCHAR(15);
        DECLARE @LoaiThe NVARCHAR(50);
        DECLARE @MaPhieu VARCHAR(15);
        DECLARE @TienGiamGia INT;
        DECLARE @NgayBatDau DATE = GETDATE();
        DECLARE @NgayKetThuc DATE = DATEADD(MONTH, 1, @NgayBatDau);

        DECLARE KhachHangCursor CURSOR FOR
        SELECT SDT, LoaiThe FROM @DanhSachKhachHang;

        OPEN KhachHangCursor;

        FETCH NEXT FROM KhachHangCursor INTO @SDT, @LoaiThe;

        WHILE @@FETCH_STATUS = 0
        BEGIN
			BEGIN TRANSACTION;

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

            -- Sinh mã phiếu giảm giá ngẫu nhiên
            SET @MaPhieu = 'PGG' + RIGHT(CAST(ABS(CHECKSUM(NEWID())) AS VARCHAR), 6);

            -- Gửi phiếu giảm giá vào bảng PHIEUGIAMGIA
            INSERT INTO PHIEUGIAMGIA WITH (HOLDLOCK, ROWLOCK)
            VALUES (@SDT, @MaPhieu, @TienGiamGia, @NgayBatDau, @NgayKetThuc);

            FETCH NEXT FROM KhachHangCursor INTO @SDT, @LoaiThe;

			COMMIT TRANSACTION;
        END;

        CLOSE KhachHangCursor;
        DEALLOCATE KhachHangCursor;

        
        PRINT N'Gửi phiếu giảm giá sinh nhật thành công.';
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        PRINT N'Lỗi trong quá trình gửi phiếu giảm giá sinh nhật: ' + ERROR_MESSAGE();
    END CATCH
END
GO

----------------------------------------------------BỘ PHẬN QUẢN LÝ NGÀNH HÀNG---------------------------------
CREATE OR ALTER PROC usp_QuanLyDanhMuc
    @MaDanhMuc NVARCHAR(15),
    @TenDanhMuc NVARCHAR(30),
    @Action CHAR(1)
AS
BEGIN
    BEGIN TRANSACTION;
    SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; 

    BEGIN TRY
        IF (@Action = 'C')
        BEGIN
            INSERT INTO DANHMUC WITH (TABLOCK)
            VALUES (@MaDanhMuc, @TenDanhMuc);
        END
        ELSE IF (@Action = 'U')
        BEGIN
            UPDATE DANHMUC WITH (HOLDLOCK, ROWLOCK)
            SET TenDanhMuc = @TenDanhMuc
            WHERE IDDanhMuc = @MaDanhMuc
            
        END
        ELSE IF (@Action = 'D')
        BEGIN
            DELETE FROM DANHMUC WITH (HOLDLOCK, ROWLOCK)
            WHERE IDDanhMuc = @MaDanhMuc
            
        END

        COMMIT TRANSACTION;
        PRINT N'Procedure usp_QuanLyDanhMuc thành công.';
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        PRINT N'Lỗi trong usp_QuanLyDanhMuc: ' + ERROR_MESSAGE();
    END CATCH
END
GO

CREATE OR ALTER PROCEDURE usp_ThemSanPham
    @MaSanPham NVARCHAR(15),
    @NSX NVARCHAR(100),
    @IDDanhMuc NVARCHAR(15),
    @TenSanPham NVARCHAR(50),
    @Gia INT
AS
BEGIN
    BEGIN TRANSACTION;
    SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;

    BEGIN TRY
        IF EXISTS (SELECT 1 FROM SANPHAM WITH (HOLDLOCK, ROWLOCK) WHERE IDSanPham = @MaSanPham)
        BEGIN
            RAISERROR(N'Sản phẩm đã tồn tại.', 16, 1);
            RETURN;
        END

        INSERT INTO SANPHAM WITH (TABLOCK)
        VALUES (@MaSanPham, @NSX, @IDDanhMuc, @TenSanPham, @Gia);

        COMMIT TRANSACTION;
        PRINT N'Thêm sản phẩm thành công.';
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        PRINT N'Lỗi khi thêm sản phẩm: ' + ERROR_MESSAGE();
    END CATCH
END
GO

CREATE OR ALTER PROCEDURE usp_XoaSanPham
    @MaSanPham NVARCHAR(15)
AS
BEGIN
    BEGIN TRANSACTION;
    SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; 

    BEGIN TRY
        IF NOT EXISTS (SELECT 1 FROM SANPHAM WITH (HOLDLOCK, ROWLOCK) WHERE IDSanPham = @MaSanPham)
        BEGIN
            RAISERROR(N'Sản phẩm không tồn tại.',  16, 1);
            RETURN;
        END

        DELETE FROM SANPHAM WITH (HOLDLOCK, ROWLOCK) WHERE IDSanPham = @MaSanPham 

        COMMIT TRANSACTION;
        PRINT N'Xóa sản phẩm thành công.';
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        PRINT N'Lỗi khi xóa sản phẩm: ' + ERROR_MESSAGE();
    END CATCH
END
GO

CREATE OR ALTER PROCEDURE usp_CapNhatSanPham
    @MaSanPham NVARCHAR(15),
    @NSX NVARCHAR(100),
    @IDDanhMuc NVARCHAR(15),
    @TenSanPham NVARCHAR(50),
    @Gia INT
AS
BEGIN
    BEGIN TRANSACTION;
    SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; 

    BEGIN TRY
        IF NOT EXISTS (SELECT 1 FROM SANPHAM WITH (HOLDLOCK, ROWLOCK) WHERE IDSanPham = @MaSanPham)
        BEGIN
            RAISERROR(N'Sản phẩm không tồn tại.', 16, 1);
            RETURN;
        END

        IF NOT EXISTS (SELECT 1 FROM DANHMUC WITH (HOLDLOCK, ROWLOCK) WHERE IDDanhMuc = @IDDanhMuc)
        BEGIN
            RAISERROR(N'Danh mục không tồn tại.', 16, 1);
            RETURN;
        END

        UPDATE SANPHAM WITH (HOLDLOCK, ROWLOCK)
        SET NSX = @NSX, IDDanhMuc = @IDDanhMuc, TenSanPham = @TenSanPham, GiaNiemYet = @Gia
        WHERE IDSanPham = @MaSanPham 

        COMMIT TRANSACTION;
        PRINT N'Cập nhật sản phẩm thành công.';
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        PRINT N'Lỗi khi cập nhật sản phẩm: ' + ERROR_MESSAGE();
    END CATCH
END
GO

CREATE OR ALTER FUNCTION usp_KiemTraSoLuongTonKho (@IDSanPham varchar(15))
RETURNS INT
AS
BEGIN
    DECLARE @SL int
    SELECT @SL = SL_HT
    FROM KhoHang WITH (HOLDLOCK, ROWLOCK) 
    WHERE IDSanPham = @IDSanPham
    RETURN @SL
END
GO

CREATE OR ALTER PROC usp_ThietLapChuongTrinhFlashSale
    @MaGiamGia VARCHAR(15),
    @TiLeGiamGia INT,
    @NgayBatDau DATE,
    @NgayKetThuc DATE,
    @SoLuongGiam INT,
    @IDSanPham VARCHAR(15)
AS
BEGIN
    BEGIN TRANSACTION
    SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; 

    BEGIN TRY 
        IF EXISTS (SELECT * FROM GIAMGIA WITH (HOLDLOCK, ROWLOCK) WHERE MaGiamGia = @MaGiamGia)
        BEGIN
            RAISERROR(N'Giảm giá đã tồn tại.', 16, 1);
            RETURN;
        END

        IF NOT EXISTS (SELECT 1 FROM SANPHAM WITH (HOLDLOCK, ROWLOCK) WHERE IDSanPham = @IDSanPham)
        BEGIN
            RAISERROR(N'Sản phẩm không tồn tại.', 16, 1);
            RETURN;
        END

        IF @NgayBatDau > @NgayKetThuc  
        BEGIN
            RAISERROR(N'Ngày bắt đầu ở sau ngày kết thúc.', 16, 1);
            RETURN;
        END

        IF @SoLuongGiam > dbo.usp_KiemTraSoLuongTonKho(@IDSanPham)
        BEGIN
            RAISERROR(N'Số lượng tồn kho ít hơn số lượng muốn giảm', 16, 1);
            RETURN;
        END

        INSERT INTO GIAMGIA WITH (TABLOCK)
        VALUES(@MaGiamGia, @TiLeGiamGia, @NgayBatDau, @NgayKetThuc, @SoLuongGiam, 1)

        INSERT INTO FlashSale WITH (TABLOCK)
        VALUES(@MaGiamGia, @IDSanPham)

        COMMIT TRANSACTION;
        PRINT N'Thêm Flash Sale thành công.';
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        PRINT N'Lỗi khi thêm Flash Sale: ' + ERROR_MESSAGE();
    END CATCH
END
GO

CREATE OR ALTER PROC usp_ThietLapChuongTrinhComboSale
    @MaGiamGia VARCHAR(15),
    @TiLeGiamGia INT,
    @NgayBatDau DATE,
    @NgayKetThuc DATE,
    @SoLuongGiam INT,
    @IDSanPham1 VARCHAR(15),
    @IDSanPham2 VARCHAR(15)
AS
BEGIN
    BEGIN TRANSACTION
    SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;

    BEGIN TRY 
        IF EXISTS (SELECT * FROM GIAMGIA WITH (HOLDLOCK, ROWLOCK) WHERE MaGiamGia = @MaGiamGia)
        BEGIN
            RAISERROR(N'Giảm giá đã tồn tại.', 16, 1);
            RETURN;
        END


        IF NOT EXISTS (SELECT 1 FROM SANPHAM WITH (HOLDLOCK, ROWLOCK) WHERE IDSanPham = @IDSanPham1)
        BEGIN
            RAISERROR(N'Sản phẩm 1 không tồn tại.', 16, 1);
            RETURN;
        END

        IF NOT EXISTS (SELECT 1 FROM SANPHAM WITH (HOLDLOCK, ROWLOCK) WHERE IDSanPham = @IDSanPham2)
        BEGIN
            RAISERROR(N'Sản phẩm 2 không tồn tại.', 16, 1);
            RETURN;
        END

        IF @NgayBatDau > @NgayKetThuc  
        BEGIN
            RAISERROR(N'Ngày bắt đầu ở sau ngày kết thúc.', 16, 1);
            RETURN;
        END

        IF @SoLuongGiam > dbo.usp_KiemTraSoLuongTonKho(@IDSanPham1)
        BEGIN
            RAISERROR(N'Số lượng tồn kho sản phẩm 1 ít hơn số lượng muốn giảm', 16, 1);
            RETURN;
        END

        BEGIN
            IF @SoLuongGiam > dbo.usp_KiemTraSoLuongTonKho(@IDSanPham2)
            BEGIN
                RAISERROR(N'Số lượng tồn kho sản phẩm 2 ít hơn số lượng muốn giảm', 16, 1);
                RETURN;
            END
        END

        INSERT INTO GIAMGIA WITH (TABLOCK)
        VALUES(@MaGiamGia, @TiLeGiamGia, @NgayBatDau, @NgayKetThuc, @SoLuongGiam, 1)

        INSERT INTO ComboSale WITH (TABLOCK)
        VALUES(@MaGiamGia, @IDSanPham1, @IDSanPham2)

        COMMIT TRANSACTION;
        PRINT N'Thêm Combo Sale thành công.';
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        PRINT N'Lỗi khi thêm Combo Sale: ' + ERROR_MESSAGE();
    END CATCH
END
GO

CREATE OR ALTER PROC usp_ThietLapChuongTrinhMemberSale
    @MaGiamGia VARCHAR(15),
    @TiLeGiamGia INT,
    @NgayBatDau DATE,
    @NgayKetThuc DATE,
    @SoLuongGiam INT,
    @IDSanPham VARCHAR(15),
    @MemberRank NVARCHAR(50)
AS
BEGIN
    BEGIN TRANSACTION
    SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; 

    BEGIN TRY 
        IF EXISTS (SELECT * FROM GIAMGIA WITH (HOLDLOCK, ROWLOCK) WHERE MaGiamGia = @MaGiamGia)
        BEGIN
            RAISERROR(N'Giảm giá đã tồn tại.', 16, 1);
            RETURN;
        END

        IF NOT EXISTS (SELECT 1 FROM SANPHAM WITH (HOLDLOCK, ROWLOCK) WHERE IDSanPham = @IDSanPham)
        BEGIN
            RAISERROR(N'Sản phẩm không tồn tại.', 16, 1);
            RETURN;
        END

        IF @NgayBatDau > @NgayKetThuc  
        BEGIN
            RAISERROR(N'Ngày bắt đầu ở sau ngày kết thúc.', 16, 1);
            RETURN;
        END

        IF @SoLuongGiam > dbo.usp_KiemTraSoLuongTonKho(@IDSanPham)
        BEGIN
            RAISERROR(N'Số lượng tồn kho sản phẩm ít hơn số lượng muốn giảm', 16, 1);
            RETURN;
        END

        IF @MemberRank NOT IN (N'Thân Thiết', N'Đồng', N'Bạc', N'Vàng', N'Bạch Kim', N'Kim Cương')
        BEGIN
            RAISERROR(N'Hạng để lập sale không tồn tại', 16, 1);
            RETURN;
        END

        INSERT INTO GIAMGIA WITH (TABLOCK)
        VALUES(@MaGiamGia, @TiLeGiamGia, @NgayBatDau, @NgayKetThuc, @SoLuongGiam, 1)

        INSERT INTO MemberSale WITH (TABLOCK)
        VALUES(@MaGiamGia, @IDSanPham, @MemberRank)

        COMMIT TRANSACTION;
        PRINT N'Thêm Member Sale thành công.';
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        PRINT N'Lỗi khi thêm Member Sale: ' + ERROR_MESSAGE();
    END CATCH
END
GO

GO
CREATE OR ALTER PROC usp_KiemTraHoatDongSale
	@MaGiamGia VARCHAR(15)
AS
BEGIN
    BEGIN TRANSACTION
    SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;

    BEGIN TRY
        UPDATE GIAMGIA WITH (HOLDLOCK, ROWLOCK)
        SET TrangThaiSale = 0
        WHERE SoLuongGiam = 0 OR NgayKetThucGiam < GETDATE() AND MaGiamGia = @MaGiamGia
        COMMIT TRANSACTION
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        PRINT N'Lỗi khi cập nhật tình trạng Sale: ' + ERROR_MESSAGE();
    END CATCH
END
GO

GO
CREATE OR ALTER PROC usp_TimMonTheoDanhMuc
	@IDDanhMuc VARCHAR(15)
AS
BEGIN
    BEGIN TRANSACTION
    SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;

    BEGIN TRY
       IF NOT EXISTS (SELECT 1 FROM DANHMUC WHERE IDDanhMuc = @IDDanhMuc)
	   BEGIN
			RAISERROR(N'Không tồn tại danh mục', 16,1);
			RETURN
	   END

	   SELECT S.*
	   FROM SANPHAM S
	   WHERE IDDanhMuc = @IDDanhMuc

	   COMMIT TRANSACTION
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        PRINT N'Lỗi khi tìm kiếm món ăn theo danh mục: ' + ERROR_MESSAGE();
    END CATCH
END
GO

----------------------------------------------------BỘ PHẬN XỬ LÝ ĐƠN HÀNG---------------------------------
-------------------------------------------------------------TYPE------------------------------------------
CREATE TYPE ChiTietDonHang AS TABLE
(
	IDSanPham varchar(15),
	Quantity int
);
-------------------------------------------------------------PROC------------------------------------------
GO
CREATE OR ALTER PROCEDURE usp_XuLyDonHang
    @MaDonHang VARCHAR(15),
    @SDT VARCHAR(15),
	@TongGiaTriDonHang BIGINT
AS
BEGIN
    SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;

    BEGIN TRY
        -- Bắt đầu transaction
        BEGIN TRANSACTION;

        DECLARE @TienGiamGia INT;

        -- Kiểm tra xem có phiếu quà tặng không
        SELECT TOP 1 @TienGiamGia = TienGiamGia
        FROM PHIEUGIAMGIA WITH (HOLDLOCK, ROWLOCK)
        WHERE SDT = @SDT AND GETDATE() BETWEEN NgayBatDau AND NgayKetThuc


        -- Nếu có phiếu quà tặng, tính toán tổng giá trị đơn hàng
        IF @TienGiamGia IS NOT NULL
        BEGIN
            IF @TongGiaTriDonHang < @TienGiamGia
                SET @TienGiamGia = @TongGiaTriDonHang; -- Không để tổng giá trị âm
        END
        ELSE
        BEGIN
            SET @TienGiamGia = 0; -- Không có phiếu quà tặng
        END

        -- Tính tổng giá trị đơn hàng sau khi áp dụng phiếu quà tặng
		DECLARE @GiaTien BIGINT
        SET @GiaTien = @TongGiaTriDonHang - @TienGiamGia;

        -- Cập nhật giá trị tổng hóa đơn vào Đơn hàng
        UPDATE DonHang WITH (ROWLOCK, HOLDLOCK)
        SET TongGiaTriDonHang = @GiaTien, TinhTrangDonHang = N'Đã thành công'
        WHERE MaDonHang = @MaDonHang;

        -- Xóa phiếu tặng đã sử dụng
        IF @TienGiamGia <> 0
        BEGIN
            DELETE FROM PHIEUGIAMGIA
            WHERE SDT = @SDT AND TienGiamGia = @TienGiamGia;
        END

        -- Thông báo về giá trị của đơn hàng
        PRINT N'Tổng giá trị đơn hàng: ' + CAST(@GiaTien AS NVARCHAR(20));

        -- Commit transaction
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        -- Rollback transaction nếu có lỗi xảy ra
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        -- Xử lý lỗi
        PRINT N'Có lỗi xảy ra: ' + ERROR_MESSAGE();
    END CATCH
END;

GO
CREATE OR ALTER FUNCTION usp_CheckCombo(@IDSanpham varchar(15), @ChiTietDonHang ChiTietDonHang READONLY)
RETURNS BIT
BEGIN
	IF EXISTS (
        SELECT 1
        FROM @ChiTietDonHang CTDH, ComboSale CS
        WHERE CTDH.IDSanPham = CS.IDSanPham1
		AND @IDSanpham = CS.IDSanPham2
    ) OR
	EXISTS (
        SELECT 1
        FROM @ChiTietDonHang CTDH, ComboSale CS
        WHERE CTDH.IDSanPham = CS.IDSanPham2
		AND @IDSanpham = CS.IDSanPham1
    )
	BEGIN
		RETURN 1;
	END
	RETURN 0;
END

GO
CREATE OR ALTER PROCEDURE usp_CapNhatSL_HT
	@IDSanPham VARCHAR(15),
	@SoLuong int
AS
BEGIN
	BEGIN TRANSACTION
	SET TRANSACTION ISOLATION LEVEL REPEATABLE READ
	
	DECLARE @SLHT int

	SELECT @SLHT = SL_HT
	FROM KhoHang WITH (XLOCK)
	WHERE IDSanPham = @IDSanPham

	SET @SLHT -= @SoLuong

	UPDATE KhoHang WITH (ROWLOCK, HOLDLOCK)
	SET SL_HT = @SLHT
	WHERE IDSanPham = @IDSanPham
	COMMIT TRANSACTION
END

GO
CREATE OR ALTER PROCEDURE usp_TaoDonHang
    @ChiTietDonHang ChiTietDonHang READONLY,
    @SDT VARCHAR(15),
    @isOnline NVARCHAR(50),
    @CachThanhToan NVARCHAR(50),
    @diaChiGiaoHang NVARCHAR(50),
    @Ketqua NVARCHAR(MAX) OUTPUT
AS
BEGIN
    SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;

    DECLARE @MaDonHang VARCHAR(15);
    DECLARE @TongGiaTriDonHang BIGINT = 0;
    DECLARE @GiaSanPham INT;
    DECLARE @SoLuong INT;
    DECLARE @KhuyenMaiTiLe INT;
    DECLARE @KhuyenMaiMa VARCHAR(15);
    DECLARE @IDSanPham VARCHAR(15);
    DECLARE @SL_TonKho INT;
    DECLARE @IsKhachHangThe BIT;

    BEGIN TRY
        -- Kiểm tra xem số điện thoại có trong bảng KHACHHANGTHE không
        SELECT @IsKhachHangThe = CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
        FROM KHACHHANGTHE
        WHERE SDT = @SDT;

        -- Tạo mã đơn hàng mới
		SELECT @MaDonHang = 'DH' + RIGHT('000' + CAST(ISNULL(MAX(CAST(SUBSTRING(MaDonHang, 3, LEN(MaDonHang) - 2) AS INT)), 0) + 1 AS VARCHAR(3)), 3)
		FROM DonHang;

        -- Thêm đơn hàng mới vào bảng DonHang
        INSERT INTO DonHang WITH (TABLOCK)
        VALUES (@MaDonHang, @SDT, GETDATE(), NULL, @CachThanhToan, N'Đang xử lý', @isOnline, @diaChiGiaoHang, N'Chưa giao');

        -- Duyệt qua từng sản phẩm trong @ChiTietDonHang
        DECLARE cur CURSOR FOR
        SELECT IDSanPham, Quantity FROM @ChiTietDonHang;

        OPEN cur;
        FETCH NEXT FROM cur INTO @IDSanPham, @SoLuong;

        WHILE @@FETCH_STATUS = 0
        BEGIN
			BEGIN TRANSACTION;

            -- Kiểm tra số lượng tồn kho
            SELECT @SL_TonKho = SL_HT FROM KhoHang WITH (HOLDLOCK, ROWLOCK) WHERE IDSanPham = @IDSanPham;

            IF @SL_TonKho < @SoLuong
            BEGIN
                RAISERROR(N'Số lượng sản phẩm %s không đủ trong kho.', 16, 1, @IDSanPham);
                ROLLBACK TRANSACTION;
                RETURN;
            END

            -- Kiểm tra khuyến mãi
            SET @KhuyenMaiMa = NULL;
            SET @KhuyenMaiTiLe = NULL;

            -- Flash Sale
            SELECT TOP 1 @KhuyenMaiMa = G.MaGiamGia, @KhuyenMaiTiLe = G.TiLeGiamGia
            FROM FlashSale FS WITH (HOLDLOCK, ROWLOCK)
            JOIN GIAMGIA G WITH (HOLDLOCK, ROWLOCK)
			ON FS.MaGiamGia = G.MaGiamGia 
            WHERE FS.IDSanPham = @IDSanPham AND G.TrangThaiSale = 1

            -- Nếu không có Flash Sale, kiểm tra Combo Sale
            IF @KhuyenMaiMa IS NULL 
            BEGIN
				IF dbo.usp_CheckCombo(@IDSanPham, @ChiTietDonHang) = 1
				BEGIN
					SELECT TOP 1 @KhuyenMaiMa = G.MaGiamGia, @KhuyenMaiTiLe = G.TiLeGiamGia
					FROM ComboSale CS WITH (HOLDLOCK, ROWLOCK)
					JOIN GIAMGIA G WITH (HOLDLOCK, ROWLOCK)
					ON CS.MaGiamGia = G.MaGiamGia
					WHERE (CS.IDSanPham1 = @IDSanPham OR CS.IDSanPham2 = @IDSanPham)
					AND G.TrangThaiSale = 1
				END
            END

            -- Nếu không có khuyến mãi nào, kiểm tra Member Sale nếu có KHACHHANGTHE
            IF @KhuyenMaiMa IS NULL AND @IsKhachHangThe = 1
            BEGIN
                SELECT TOP 1 @KhuyenMaiMa = G.MaGiamGia, @KhuyenMaiTiLe = G.TiLeGiamGia
                FROM MemberSale MS WITH (HOLDLOCK, ROWLOCK)
                JOIN GIAMGIA G WITH (HOLDLOCK, ROWLOCK)
				ON MS.MaGiamGia = G.MaGiamGia
                WHERE MS.IDSanPham = @IDSanPham AND G.TrangThaiSale = 1
            END

            -- Tính giá trị đơn hàng
            SELECT @GiaSanPham = GiaNiemYet FROM SanPham WHERE IDSanPham = @IDSanPham;

            IF @KhuyenMaiTiLe IS NOT NULL
            BEGIN
                SET @GiaSanPham = @GiaSanPham * (1 - @KhuyenMaiTiLe / 100.0);
            END

            SET @TongGiaTriDonHang = @TongGiaTriDonHang + (@GiaSanPham * @SoLuong);

            -- Cập nhật số lượng tồn kho
            exec usp_CapNhatSL_HT @IDSanPham, @SoLuong

			IF(@KhuyenMaiMa IS NOT NULL)
			BEGIN
				UPDATE GIAMGIA WITH (ROWLOCK, HOLDLOCK)
				SET SoLuongGiam -= @SoLuong
				WHERE MaGiamGia = @KhuyenMaiMa
			END

			INSERT INTO SanPhamDonHang WITH (TABLOCK)
			VALUES (@MaDonHang, @IDSanPham, @SoLuong);

            FETCH NEXT FROM cur INTO @IDSanPham, @SoLuong;
			COMMIT TRANSACTION;
        END

        CLOSE cur;
        DEALLOCATE cur;

        -- Cập nhật tổng giá trị đơn hàng
		exec usp_XuLyDonHang @MaDonHang, @SDT, @TongGiaTriDonHang

        SET @Ketqua = N'Tạo đơn hàng thành công với mã: ' + @MaDonHang;
        
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        SET @Ketqua = ERROR_MESSAGE();
    END CATCH
END;

GO


CREATE OR ALTER PROCEDURE usp_TraLaiDonHang
	@MaDonHang VARCHAR(15)
AS
BEGIN
	BEGIN TRY
	
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
END


GO
CREATE OR ALTER PROCEDURE usp_UpdateSanPhamDonHang
	@MaDonHang VARCHAR(15),
	@IDSanPham VARCHAR(15),
	@Soluong INT
AS
BEGIN
	BEGIN TRY
	BEGIN TRANSACTION 
	SET TRANSACTION ISOLATION LEVEL SERIALIZABLE

	IF NOT EXISTS (SELECT 1 FROM SanPhamDonHang WHERE IDSanPham = @IDSanPham AND MaDonHang = @MaDonHang)
	BEGIN
		RAISERROR(N'Không tồn tại đơn hàng có sản phẩm đó', 16,1)
		RETURN
	END

	DECLARE @SoLuongBienThien INT

	SELECT @SoLuongBienThien = @Soluong - SoLuong
	FROM SanPhamDonHang WITH (READCOMMITTED)
	WHERE MaDonHang = @MaDonHang AND IDSanPham = @IDSanPham
	
	UPDATE SanPhamDonHang WITH (UPDLOCK)
	SET SoLuong = @Soluong
	WHERE MaDonHang = @MaDonHang AND IDSanPham = @IDSanPham

	UPDATE DonHang WITH (UPDLOCK)
	SET TongGiaTriDonHang += (SELECT @SoLuongBienThien * GiaNiemYet FROM SANPHAM WHERE IDSanPham = @IDSanPham)
	WHERE MaDonHang = @MaDonHang

	COMMIT TRANSACTION

	END TRY
	BEGIN CATCH
	PRINT(N'Lỗi:' + ERROR_MESSAGE());
	ROLLBACK TRANSACTION
	END CATCH
END

GO
CREATE OR ALTER PROCEDURE usp_XoaSanPhamDonHang
	@MaDonHang VARCHAR(15),
	@IDSanPham VARCHAR(15)
AS
BEGIN
	BEGIN TRY
	BEGIN TRANSACTION 
	SET TRANSACTION ISOLATION LEVEL SERIALIZABLE

	IF NOT EXISTS (SELECT 1 FROM SanPhamDonHang WITH (READCOMMITTED) WHERE IDSanPham = @IDSanPham AND MaDonHang = @MaDonHang)
	BEGIN
		RAISERROR(N'Không tồn tại đơn hàng có sản phẩm đó', 16,1)
		RETURN
	END

	UPDATE DonHang WITH (UPDLOCK)
	SET TongGiaTriDonHang -= (SELECT sp.SoLuong * s.GiaNiemYet FROM SANPHAM s JOIN SanPhamDonHang sp ON s.IDSanPham = sp.IDSanPham WHERE sp.IDSanPham = @IDSanPham AND sp.MaDonHang = @MaDonHang)

	DELETE SanPhamDonHang WITH (ROWLOCK, HOLDLOCK)
	WHERE MaDonHang = @MaDonHang AND IDSanPham = @IDSanPham 

	COMMIT TRANSACTION
	END TRY
	BEGIN CATCH
	PRINT(N'Lỗi:' + ERROR_MESSAGE());
	ROLLBACK TRANSACTION
	END CATCH
END

GO
CREATE OR ALTER PROCEDURE usp_ThemSanPhamDonHang
	@MaDonHang VARCHAR(15),
	@IDSanPham VARCHAR(15),
	@Soluong INT
AS
BEGIN
	BEGIN TRY
	BEGIN TRANSACTION 
	SET TRANSACTION ISOLATION LEVEL SERIALIZABLE

	IF EXISTS (SELECT 1 FROM SanPhamDonHang WHERE IDSanPham = @IDSanPham AND MaDonHang = @MaDonHang)
	BEGIN
		RAISERROR(N'Đã tồn tại đơn hàng có sản phẩm đó', 16,1)
		RETURN
	END

	INSERT INTO SanPhamDonHang WITH (TABLOCK) 
	VALUES (@MaDonHang, @IDSanPham, @Soluong)

	UPDATE DonHang WITH (UPDLOCK)
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

----------------------------------------------------BỘ PHẬN QUẢN LÝ KHO HÀNG---------------------------------








----------------------------------------------------BỘ PHẬN KINH DOANH---------------------------------------

go
create or alter proc usp_KhachHangDoanhThu
	@Ngay Date
as
begin 
	begin tran
		set tran isolation level serializable
		if not exists (
						select 1
						from DonHang with (rowlock, holdlock)
						where NgayBan = @Ngay
					  )
			begin
				;throw 50000, N'Ngày này không có người mua hàng', 1
				return
			end

		-- Cho bộ phận A
		select count(*) as N'Tổng lượng khách hàng', sum(TongGiaTriDonHang) as N'Tổng doanh thu'
		from DonHang with (rowlock, holdlock)
		where NgayBan = @Ngay and TinhTrangDonHang <> N'Trả lại'
		group by NgayBan

		waitfor delay '00:00:10'

		-- Cho bộ phận B
		select count(*) as N'Tổng lượng khách hàng', sum(TongGiaTriDonHang) as N'Tổng doanh thu'
		from DonHang with (rowlock, holdlock)
		where NgayBan = @Ngay and TinhTrangDonHang <> N'Trả lại'
		group by NgayBan

	commit tran
end

--exec dbo.usp_KhachHangDoanhThu '2023-11-20'

go 
create or alter proc usp_SoLuongDaBanVaKhachMua
	@Ngay Date
as
begin
	begin tran
		set tran isolation level read committed
		if not exists (
						select 1
						from DonHang
						where NgayBan = @Ngay
					)
			begin
				;throw 50000, N'Ngày này không có người mua hàng', 1
				return
			end

		select spdh.IDSanPham as N'Sản phẩm', count(distinct dh.SDT) as N'Số lượng khách mua', sum(spdh.SoLuong) as N'Số lượng bán ra'
		from SanPhamDonHang spdh with (rowlock) join DonHang dh with (rowlock) on spdh.MaDonHang = dh.MaDonHang 
		where dh.TinhTrangDonHang <> N'Trả lại' and NgayBan = @Ngay
		group by spdh.IDSanPham
		order by sum(spdh.SoLuong) desc
	commit tran
end

go
create or alter proc usp_ThongKeTraHang
	@MaDh varchar(15)
as
begin
	begin tran 
	set tran isolation level repeatable read
		select spdh.IDSanPham as N'Sản phẩm', sum(spdh.SoLuong) as N'Số lượng'
		from DonHang dh 
		with (repeatableread) 
		join SanPhamDonHang spdh 
		with (repeatableread)
		on dh.MaDonHang = spdh.MaDonHang
		where @MaDh = dh.MaDonHang and dh.TinhTrangDonHang = N'Trả lại'
		group by spdh.IDSanPham

		waitfor delay '00:00:10'

		select spdh.IDSanPham as N'Sản phẩm', sum(spdh.SoLuong) as N'Số lượng bị trả'
		from DonHang dh 
		with (repeatableread)
		join SanPhamDonHang spdh
		with (repeatableread)
		on dh.MaDonHang = spdh.MaDonHang
		where  @MaDh = dh.MaDonHang and dh.TinhTrangDonHang = N'Trả lại'
		group by spdh.IDSanPham

	commit tran
end

go
create or alter proc usp_ThangMuaSam
as
begin
	begin tran
		set tran isolation level serializable
		select distinct spdh.IDSanPham, MONTH(dh.NgayBan) as N'Tháng', sum(spdh.SoLuong) as N'Số lượng bán ra'
		from DonHang dh with (tablock) join SanPhamDonHang spdh with (tablock) on dh.MaDonHang = spdh.MaDonHang 
		where dh.TinhTrangDonHang <> N'Trả lại'
		group by spdh.IDSanPham, month(dh.NgayBan)
		having sum(spdh.SoLuong) >= all (
											select sum(spdh2.SoLuong)
											from DonHang dh2 with (tablock) join SanPhamDonHang spdh2 with (tablock) on dh2.MaDonHang = spdh2.MaDonHang
											where dh2.TinhTrangDonHang <> N'Trả lại'
											and MONTH(dh2.NgayBan) = MONTH(dh.NgayBan)
											group by spdh2.IDSanPham
										)
	commit tran
end
go