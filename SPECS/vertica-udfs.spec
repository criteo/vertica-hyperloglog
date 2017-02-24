Name:           vertica-udfs
Version:        0.1
Release:        1%{?dist}
Summary:        HyperLogLog UDF for Vertica

License:        GPLv2+
URL:            https://gitlab.criteois.com/rpm-packages/%{name}
Source0:        https://gitlab.criteois.com/rpm-packages/%{name}/repository/archive.tar.gz
BuildRoot:      %{_tmppath}/%{name}-%{version}

BuildRequires:  gcc-c++
BuildRequires:  make
BuildRequires:  cmake
BuildRequires:  openssh
BuildRequires:  dialog
BuildRequires:  vertica
Requires:       vertica

%description
For complete documentation, see the project home page on GitHub.

%prep
%setup -q # -c %{name}-%{version}

%build
%cmake -DSDK_HOME='/opt/vertica/sdk' -DBUILD_TESTS=ON vertica-udfs-master-*
make %{?_smp_mflags}

%install
rm -rf $RPM_BUILD_ROOT
%make_install

%clean
rm -rf $RPM_BUILD_ROOT

%files
#%defattr(755, root, root, 755)
#/opt/vertica/lib/libhll.so

%check
cat vertica-udfs-master-*/CMakeLists.txt
make check

%doc

%changelog
