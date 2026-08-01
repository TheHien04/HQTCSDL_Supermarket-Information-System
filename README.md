<p align="center">
  <img src="Image/Market.jpg" alt="Supermarket information system" width="100%"/>
</p>

# Supermarket Information System  
### A Transactional Database Design and Concurrency Control Study

**Course:** Database Management Systems (Hệ Quản Trị Cơ Sở Dữ Liệu)  
**Institution:** Faculty of Information Technology, University of Science, VNU-HCM  
**Department:** Information Systems  
**Platform:** Microsoft SQL Server · Transact-SQL (T-SQL)

---

## Abstract

This repository presents a relational database implementation for **Supermarket ABC**, a retail enterprise that operates both online and offline sales channels. The system models core business subsystems—customer relationship management, product cataloguing, promotional campaigns, order processing, inventory control, and commercial analytics—while emphasizing the transactional guarantees required of a production-grade database management system.

Beyond schema design and stored-procedure development, the project systematically investigates **concurrent execution anomalies** (lost update, phantom read, and non-repeatable read), and demonstrates how **isolation levels**, **explicit locking hints**, and carefully structured transactions can preserve data consistency under contention. The artifact therefore serves both as an operational prototype and as an empirical study of concurrency control in Microsoft SQL Server.

---

## 1. Problem Statement

Supermarket ABC sells consumer goods across Ho Chi Minh City through dual retail channels. Daily operations involve high-frequency updates to shared resources: membership tiers, promotional quotas, order totals, and warehouse stock. Without rigorous transaction design, concurrent procedures can produce incorrect inventory balances, inconsistent loyalty rankings, or distorted revenue reports.

The assignment therefore requires:

1. A normalized relational model satisfying all stated business constraints.  
2. Encoded integrity rules inside stored procedures (primary/unique/foreign keys and domains in DDL; remaining business rules in procedures—not triggers).  
3. Identification and classification of concurrent conflict pairs.  
4. Isolation- and lock-based resolutions, validated through reproducible conflict scenarios with sample data.

---

## 2. System Architecture

The enterprise is decomposed into five functional subsystems. Each subsystem maps to a set of stored procedures that encapsulate domain logic and concurrency policy.

| Subsystem | Responsibilities |
|---|---|
| **Customer Care** | Account lifecycle, loyalty tier adjustment (six tiers based on prior-year spend), birthday voucher issuance |
| **Category & Merchandising** | One-level product taxonomy, product CRUD, Flash-sale / Combo-sale / Member-sale campaign setup |
| **Order Processing** | Online & offline order creation, best-applicable promotion selection, gift-voucher redemption, returns |
| **Warehouse Management** | Stock monitoring against maximum capacity (`SL_SP_TD`), reorder threshold (70%), and outstanding purchase orders |
| **Business Analytics** | Daily customer/revenue aggregates, per-SKU sales volume & buyer counts, return statistics, monthly bestsellers |

### Loyalty Tier Policy

| Tier | Prior-year spending | Birthday voucher |
|---|---|---|
| Diamond (*Kim Cương*) | ≥ 50,000,000 VND | 1,200,000 VND |
| Platinum (*Bạch Kim*) | ≥ 30,000,000 VND | 700,000 VND |
| Gold (*Vàng*) | ≥ 15,000,000 VND | 500,000 VND |
| Silver (*Bạc*) | ≥ 5,000,000 VND | 200,000 VND |
| Bronze (*Đồng*) | ≥ 1,000,000 VND | 100,000 VND |
| Member (*Thân Thiết*) | Default for new accounts | 50,000 VND |

Promotion rules enforce that (i) promotional quantity never exceeds on-hand stock at campaign creation, (ii) each line item receives at most one promotion, and (iii) discounted quantity is capped at three units per promoted SKU.

---

## 3. Logical Data Model

The schema is designed in third normal form (3NF) with declarative primary and foreign keys. Principal entities include customers and loyalty cards, gift vouchers, categories and products, promotion headers with typed specializations (Flash / Combo / Member), orders and line items, warehouse balances, and purchase-order documents.

<p align="center">
  <img src="Image/Logic.png" alt="Logical schema of the supermarket database" width="820"/>
</p>
<p align="center"><em>Figure 1. Logical relational schema of database <code>HQTCSDL</code></em></p>

### Core Relations

| Relation | Description |
|---|---|
| `KHACHHANG` / `KHACHHANGTHE` | Customer identity and loyalty membership |
| `PHIEUGIAMGIA` | Issued purchase vouchers |
| `DANHMUC` / `SANPHAM` | Product taxonomy and catalog |
| `GIAMGIA`, `FlashSale`, `ComboSale`, `MemberSale` | Promotion header and typed campaign details |
| `DonHang` / `SanPhamDonHang` | Orders and line items (online / offline) |
| `KhoHang` | On-hand, capacity, and in-transit quantities |
| `PhieuDat` / `HangDat` | Replenishment orders and delivery progress |

---

## 4. Implementation Overview

### 4.1 Technology Stack

| Layer | Choice |
|---|---|
| RDBMS | Microsoft SQL Server |
| Language | Transact-SQL (T-SQL) |
| Tooling | SQL Server Management Studio (SSMS) |
| Integrity | PK / UK / FK / domain constraints in DDL; business rules in stored procedures |
| Concurrency | `SERIALIZABLE`, `REPEATABLE READ`, `READ COMMITTED`; hints such as `HOLDLOCK`, `UPDLOCK`, `XLOCK`, `ROWLOCK`, `TABLOCK` |

### 4.2 Representative Stored Procedures

| Procedure | Subsystem | Purpose |
|---|---|---|
| `usp_DieuChinhPhanHangKhachHang` | Customer Care | Recompute a single customer's loyalty tier |
| `usp_CapNhatPhanHangDauThang` | Customer Care | Batch tier refresh at month start |
| `usp_GuiPhieuMuaHangSinhNhat` | Customer Care | Issue birthday vouchers for a given birth month |
| `usp_QuanLyChuongTrinhKhuyenMai` | Merchandising | Dispatch Flash / Combo / Member campaign setup |
| `usp_ThietLapChuongTrinhFlashSale` | Merchandising | Create a flash-sale campaign under stock limits |
| `usp_ThietLapChuongTrinhComboSale` | Merchandising | Create a two-SKU combo promotion |
| `usp_ThietLapChuongTrinhMemberSale` | Merchandising | Create a member-tier-specific promotion |
| `usp_TaoDonHang` / `usp_TaoHoaDon` | Order Processing | Create an order, select best promotion, cap discount at 3 units |
| `usp_ApDungKhuyenMai_SanPham` | Order Processing | Choose the best eligible promotion for a line item |
| `usp_XuLyDonHang` | Order Processing | Finalize totals and redeem gift vouchers |
| `usp_XuLyHuyDon` | Order Processing | Cancel an order and restore inventory |
| `usp_TraLaiDonHang` | Order Processing | Process a return and restore inventory |
| `usp_CapNhatSL_HT` | Warehouse | Atomically decrement on-hand quantity |
| `usp_ChinhSuaSL_SP_TD` | Warehouse | Maintain maximum storage capacity |
| `usp_KiemTraDieuKienDatHang` | Warehouse | Enforce 70% reorder threshold, 10% minimum order, capacity cap |
| `usp_TinhHangCanDat` | Warehouse | Compute `SL_SP_TD - SL_HT - SL_CG` |
| `usp_ThucHienDatHang` / `usp_DatHangCuoiNgay` | Warehouse | Create purchase orders for under-threshold SKUs |
| `usp_CapNhatSL_HDG` / `usp_CapNhatSL_HCG` | Warehouse | Receive deliveries and update on-hand / in-transit stock |
| `usp_KhachHangDoanhThu` | Analytics | Daily customer count and revenue |
| `usp_SoLuongDaBanVaKhachMua` | Analytics | Per-SKU sales volume and distinct buyers |
| `usp_ThangMuaSam` | Analytics | Monthly bestselling products |

Procedures consistently wrap critical sections in explicit transactions, select an isolation level appropriate to the anomaly under consideration, and apply lock hints to bound the locking scope.

---

## 5. Concurrency Control Study

A central contribution of this project is the controlled reproduction of concurrency anomalies and their mitigation. Demo scripts in `Code/Nhom6_demo.sql` intentionally weaken isolation (or omit locking) so that students can observe incorrect outcomes, then compare them with the hardened procedures in `Code/Nhóm-6_procedures.sql`.

### 5.1 Lost Update

| Scenario | Contending procedures | Shared resource | Mitigation |
|---|---|---|---|
| Inventory decrement | Delayed `usp_CapNhatSL_HT` vs. production version | `KhoHang.SL_HT` | `REPEATABLE READ` + exclusive lock (`XLOCK`) on stock rows |
| Loyalty recomputation | Delayed tier update vs. order return + tier update | Derived spend & `KHACHHANGTHE.LoaiThe` | `SERIALIZABLE` + `REPEATABLEREAD` / `HOLDLOCK` on read–write sets |
| Order total rewrite | Add line item vs. remove line item | `DonHang.TongGiaTriDonHang` | `SERIALIZABLE` + `UPDLOCK` on the order header |

### 5.2 Phantom Read

| Scenario | Contending procedures | Anomaly | Mitigation |
|---|---|---|---|
| Birthday voucher batch | Voucher issuance vs. new member insert | Newly inserted birthday customer appears mid-scan | `SERIALIZABLE` + `UPDLOCK, HOLDLOCK` on customer joins |
| Month-start tier batch | Batch ranking vs. new customer + large order | Phantom member alters ranking coverage | `SERIALIZABLE` over the customer–order predicate |
| Daily revenue report | Revenue aggregation vs. order return | Aggregate counts change between two reads | `SERIALIZABLE` + `HOLDLOCK` on the daily order set |

### 5.3 Non-Repeatable Read

| Scenario | Contending procedures | Anomaly | Mitigation |
|---|---|---|---|
| Return statistics | Return report vs. line-item quantity update | Same return lines yield different quantities across two reads | `REPEATABLE READ` on `DonHang` / `SanPhamDonHang` |

These experiments illustrate the classical trade-off between **isolation strength** and **concurrency throughput**, and justify why operational procedures default to conservative locking on hot paths (inventory, loyalty, and promotional quotas).

---

## 6. Repository Structure

Submission script names follow the course specification; legacy Group-6 filenames are kept as synchronized copies.

```text
.
├── Code/
│   ├── database.sql             # DDL: database & relational schema  (required name)
│   ├── data.sql                 # Seed data (≥10 rows/table; ≥50 products; ≥30 promotions)
│   ├── procedures.sql           # Production stored procedures & concurrency controls
│   ├── demo.sql                 # Conflict reproduction + warehouse demos
│   ├── Nhóm-6_database.sql      # Synchronized copy of database.sql
│   ├── Nhóm-6_procedures.sql    # Synchronized copy of procedures.sql
│   └── Nhom6_demo.sql           # Synchronized copy of demo.sql
├── Image/
│   ├── Market.jpg               # Domain illustration
│   └── Logic.png                # Logical schema diagram
├── Report/
│   ├── Báo_cáo_lần_1_HQT.pdf    # Requirements refinement, ER/relational design, procedure catalogue
│   ├── Báo_cáo_lần_2_HQT.pdf    # Pseudocode & concurrent conflict classification
│   ├── Báo_cáo_lần_3_HQT.pdf    # Locking strategy & isolation design
│   └── Báo_cáo_lần_4_HQT.pdf    # Concrete conflict scenarios with sample data
├── HQT-CSDL_Đồ-án.pdf           # Official project specification
└── README.md
```

---

## 7. Installation and Reproduction

### Prerequisites

- Microsoft SQL Server (2019 or later recommended)  
- SQL Server Management Studio (SSMS) or any T-SQL client  

### Deployment Sequence

Execute the scripts **in order** against a local SQL Server instance:

```sql
-- 1. Create database and schema
:r Code/database.sql

-- 2. Load experimental dataset
:r Code/data.sql

-- 3. Install production procedures
:r Code/procedures.sql
```

Alternatively, open each file in SSMS and execute with `HQTCSDL` as the target database (the DDL script creates the database automatically).

### Running a Concurrency Experiment

1. Open **two** query windows connected to `HQTCSDL`.  
2. From `Code/demo.sql`, select one anomaly block (e.g., Lost Update – Scenario 1, or warehouse receiving).  
3. Create the delayed “unsafe” procedure in either window.  
4. Start the delayed procedure in Window A; within the delay window, start the contending procedure in Window B.  
5. Observe the incorrect final state (anomaly).  
6. Re-run the same workload using the production procedures (with isolation/locking enabled) and verify consistency.

### Warehouse reorder demo

```sql
DECLARE @SL INT, @OK BIT;
EXEC usp_TinhHangCanDat 'SP015', @SL OUTPUT;   -- seeded below 70% capacity
EXEC usp_ThucHienDatHang 'SP015', NULL, @OK OUTPUT;
EXEC usp_DatHangCuoiNgay;                      -- scan all under-threshold SKUs
```

---

## 8. Dataset Characteristics

The seed script provisions a compact but conflict-prone workload:

- **10** customers with loyalty cards  
- **7** product categories and **50** products  
- **50** warehouse stock rows  
- **10** completed orders with line items  
- **30** promotion headers, distributed across Flash-sale, Combo-sale, and Member-sale  
- **10** purchase-order headers with delivery progress  

Promotion validity windows and quantities are chosen so that concurrent order and campaign procedures can contend on the same SKUs and quotas.

---

## 9. Design Notes and Integrity Decisions

- **Business rules live in procedures.** Per course requirements, only key and domain constraints are declared in DDL; remaining semantic constraints (tier thresholds, promotion eligibility, stock capacity, voucher redemption) are enforced inside stored procedures.  
- **No triggers for business logic.** Automation is achieved through explicit procedure orchestration.  
- **Order status vocabulary** is normalized to `N'Thành công'` / `N'Trả lại'` / `N'Đã hủy'` / `N'Đang xử lý'` so loyalty and analytics queries remain consistent with order finalization.  
- **Customer deletion** removes dependent vouchers and loyalty rows first, and refuses deletion when historical orders still reference the customer—preserving referential integrity.  
- **Combo-sale foreign keys** reference both participating products (`IDSanPham1`, `IDSanPham2`).  
- **Member-sale tiers** use the same Vietnamese labels as loyalty cards (`Vàng`, `Bạc`, `Bạch Kim`, …).  
- **Warehouse policy:** reorder when `SL_HT < 70% * SL_SP_TD`; order quantity ≥ `10% * SL_SP_TD`; never exceed capacity after accounting for in-transit stock (`SL_CG`).  
- **Promotion selection** prefers the highest discount among currently valid Flash / Combo / Member campaigns, with at most three discounted units per SKU.

---

## 10. Authors

| Student | Student ID |
|---|---|
| Đinh Việt Đức | 22127071 |
| Nguyễn Thế Hiển | 22127107 |
| Phan Thành Quang Huy | 22127162 |
| Nguyễn Hữu Trường Sơn | 22127367 |

**Instructors:** Tuấn Nguyễn Hoài Đức · Lương Hán Cơ  

**Group:** Group 6  

---

## 11. Academic Context

This work was developed for the *Database Management Systems* course at the Faculty of Information Technology, University of Science (VNU-HCM). Assessment milestones correspond to the four progress reports in `Report/` and the final SQL script submission in `Code/`.

Suggested citation (informal):

> Group 6 (2025). *Supermarket Information System: Transactional Design and Concurrency Control on Microsoft SQL Server*. Course project, Database Management Systems, University of Science, VNU-HCM.

---

## License

Academic coursework artifact. Intended for educational use within the Database Management Systems curriculum unless otherwise stated by the authors or the hosting institution.
