Name:           vertica-udfs
Version:        0.1
Release:        1%{?dist}
Summary:        HyperLogLog UDF for Vertica

License:        GPLv2+
URL:            https://gitlab.criteois.com/vertica/vertica-udfs
Source0:        https://gitlab.criteois.com/vertica/vertica-udfs/repository/archive.tar.gz
BuildRoot:      %{_tmppath}/%{name}-%{version}

BuildRequires:  gcc-c++
BuildRequires:  make
BuildRequires:  cmake
BuildRequires:  vertica
Requires:       vertica

%description
For complete documentation, see the project home page on GitHub.

%prep
%setup -q

%build
%cmake -DSDK_HOME='/opt/vertica/sdk' .
make %{?_smp_mflags}

%install
rm -rf $RPM_BUILD_ROOT
%make_install

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(755, root, root, 755)
/opt/vertica/lib/libhll.so

%doc

%changelog
