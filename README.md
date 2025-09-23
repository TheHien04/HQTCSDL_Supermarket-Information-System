# 🛒 Hệ Thống Thông Tin Siêu Thị (Supermarket Information System)

## 📖 Giới Thiệu  

Đây là dự án môn học **Hệ Quản Trị Cơ Sở Dữ Liệu**, với mục tiêu thiết kế và xây dựng một hệ thống thông tin quản lý  
cho **Siêu thị ABC**. Hệ thống hỗ trợ hoạt động kinh doanh cả **online** và **offline**, tập trung vào việc quản lý khách hàng,  
sản phẩm, khuyến mãi, đơn hàng và kho hàng.  

Dự án giúp sinh viên vận dụng kiến thức về **thiết kế cơ sở dữ liệu quan hệ**, **chuẩn hóa dữ liệu**, **ràng buộc toàn vẹn** và  
các kỹ thuật quản trị CSDL trong bối cảnh một bài toán thực tế.  

---

## 🏗️ Các Bộ Phận Trong Hệ Thống  

1. **👩‍💼 Bộ phận chăm sóc khách hàng**  
   - Quản lý tài khoản khách hàng (tạo, sửa, phân loại)  
   - Phân hạng khách hàng (thân thiết, vãng lai, VIP)  
   - Gửi phiếu mua hàng và ưu đãi cá nhân hóa  

2. **🛍️ Bộ phận quản lý ngành hàng**  
   - Quản lý danh mục và sản phẩm của siêu thị  
   - Quản lý các chương trình khuyến mãi:  
     - Flash-sale (giảm giá theo khung giờ)  
     - Combo-sale (mua theo gói)  
     - Member-sale (ưu đãi riêng cho thành viên)  

3. **📦 Bộ phận xử lý đơn hàng**  
   - Quản lý quy trình đơn hàng online và offline  
   - Hỗ trợ khách hàng thân thiết (ưu tiên xử lý, khuyến mãi bổ sung)  
   - Quản lý thanh toán và trạng thái đơn hàng  

4. **🏭 Bộ phận quản lý kho hàng**  
   - Theo dõi số lượng tồn kho theo từng sản phẩm  
   - Cảnh báo và đặt hàng khi dưới ngưỡng cho phép  
   - Kết nối với nhà sản xuất/nhà cung cấp  

5. **📊 Bộ phận kinh doanh**  
   - Thống kê doanh thu theo thời gian, ngành hàng  
   - Báo cáo số lượng bán ra theo sản phẩm  
   - Hỗ trợ ra quyết định kinh doanh dựa trên dữ liệu  

---

## 🗄️ Mô Hình Cơ Sở Dữ Liệu  

Hệ thống sử dụng **CSDL quan hệ (Relational Database)**, với các bảng chính:  

- **Khách hàng**: Quản lý thông tin cá nhân, phân hạng, lịch sử mua hàng  
- **Sản phẩm**: Quản lý danh mục, nhà sản xuất, giá niêm yết  
- **Chương trình khuyến mãi**: Quản lý nhiều loại khuyến mãi (Flash-sale, Combo-sale, Member-sale)  
- **Đơn hàng**: Thông tin chi tiết đơn hàng, sản phẩm, khách hàng, trạng thái  
- **Kho hàng**: Quản lý số lượng tồn kho, quy trình nhập hàng từ nhà cung cấp  

Ngoài ra, hệ thống có thể triển khai thêm **ràng buộc toàn vẹn (integrity constraints)**, **trigger** và **stored procedures**  
để tự động hóa các tác vụ (ví dụ: cập nhật tồn kho khi có đơn hàng mới, kích hoạt khuyến mãi theo thời gian).  

---

## ⚙️ Các Tính Năng Chính  

- **👨‍👩‍👧‍👦 Quản lý khách hàng**  
  - Tạo tài khoản khách hàng mới  
  - Phân loại khách hàng (thân thiết, VIP, vãng lai)  
  - Gửi phiếu giảm giá, voucher  

- **📦 Quản lý sản phẩm & ngành hàng**  
  - Thêm, cập nhật, tìm kiếm sản phẩm  
  - Quản lý giá, nhà sản xuất, danh mục ngành hàng  

- **💸 Khuyến mãi**  
  - Tạo và quản lý chương trình khuyến mãi  
  - Các loại hình: Flash-sale, Combo-sale, Member-sale  
  - Liên kết khuyến mãi với nhóm sản phẩm hoặc khách hàng  

- **🛒 Quản lý đơn hàng**  
  - Tạo và xử lý đơn hàng  
  - Theo dõi trạng thái (chờ xử lý, đã thanh toán, hoàn thành)  
  - Hỗ trợ phân biệt khách hàng thân thiết và vãng lai  

- **🏭 Quản lý kho hàng**  
  - Theo dõi tồn kho theo từng sản phẩm  
  - Cảnh báo khi số lượng dưới ngưỡng  
  - Đặt hàng từ nhà cung cấp  

- **📊 Báo cáo kinh doanh**  
  - Thống kê doanh thu theo tháng, quý, năm  
  - Báo cáo sản phẩm bán chạy  
  - Phân tích hiệu quả khuyến mãi  

---

## 🎯 Kết Luận  

Dự án không chỉ củng cố kiến thức về **thiết kế và quản trị CSDL quan hệ**,  
mà còn gắn kết lý thuyết với **nghiệp vụ kinh doanh bán lẻ**.  

Hệ thống có thể được mở rộng với **ứng dụng web** hoặc **mobile app**, phục vụ cho nhu cầu quản lý siêu thị ở quy mô lớn hơn.  
