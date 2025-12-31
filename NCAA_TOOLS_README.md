# NCAA Salary Tools

This repository contains interactive HTML tools for viewing and comparing NCAA Division 1 coach salaries.

## Tools Included

### 1. NCAA Division 1 Total Compensation (`ncaa_d1_total_compensation.html`)

A comprehensive table displaying head coach salaries across NCAA Division 1 programs.

**Features:**
- Searchable table by school, coach name, or position
- Sortable columns (click any column header to sort)
- Clean, modern interface with gradient styling
- Data for 20 schools including football and basketball programs

**Usage:**
Open `ncaa_d1_total_compensation.html` in any modern web browser.

---

### 2. NCAA Salary Comparison Tool (`salary_comparison_tool.html`)

An interactive tool to compare annual salaries between two different schools.

**Features:**
- Side-by-side school selection with dropdown menus
- Manual salary input fields for each school
- Visual comparison with proportional bar charts
- Detailed salary analysis including:
  - Absolute salary difference
  - Percentage difference
  - Side-by-side comparison
  - Combined total compensation
- Beautiful gradient design with responsive layout
- Reset functionality to start a new comparison

**Usage:**
1. Open `salary_comparison_tool.html` in any modern web browser
2. Select School 1 from the first dropdown
3. Enter the annual salary amount for School 1
4. Select School 2 from the second dropdown
5. Enter the annual salary amount for School 2
6. Click "Compare Salaries" to see the results
7. Use "Reset Comparison" to start over

**Example Comparison:**
- Compare Alabama ($8,500,000) with Georgia ($9,200,000)
- The tool shows Georgia pays $700,000 (8.2%) more

---

## Data Source

All data is sourced from NCAA Public Financial Records (Updated 2024).

The dataset includes:
- **Football Programs:** 60+ NCAA Division 1 schools including Alabama, Auburn, Clemson, Florida, Georgia, LSU, Michigan, Notre Dame, Ohio State, Oklahoma, Oregon, Penn State, Tennessee, Texas, Texas A&M, USC, and many more
- **Basketball Programs:** Duke, Kansas, Kentucky, North Carolina, and others

**Note:** The comparison tool allows users to enter custom salary amounts for any school in the database.

---

## Technical Details

Both tools are:
- **Self-contained**: No external dependencies or libraries required
- **Client-side only**: All processing happens in the browser (JavaScript)
- **Responsive**: Works on desktop and mobile devices
- **Accessible**: Proper semantic HTML and ARIA labels

---

## Screenshots

### NCAA Division 1 Total Compensation Tool
![NCAA D1 Total Compensation](https://github.com/user-attachments/assets/b2e59ec5-21f1-43b9-8d1b-b387a7c67072)

### Salary Comparison Tool - Initial View
![Salary Comparison Tool Initial](https://github.com/user-attachments/assets/70e996d3-f9c9-47c2-a08c-fff522e5c948)

### Salary Comparison Tool - Results View
![Salary Comparison Results](https://github.com/user-attachments/assets/9b5ef633-c485-4def-9905-4fec53be0fa4)

---

## Browser Compatibility

These tools work in all modern browsers:
- Chrome/Edge (recommended)
- Firefox
- Safari
- Opera

---

## Future Enhancements

Potential improvements:
- Add more schools and positions
- Historical salary data comparison
- Conference-based filtering
- Export comparison results to PDF
- Integration with live NCAA data sources
