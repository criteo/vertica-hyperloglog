Name:           vertica-udfs
Version:        0.1
Release:        1%{?dist}
Summary:        HyperLogLog UDF for Vertica

License:        GPLv2+
URL:            https://gitlab.criteois.com/rpm-packages/%{name}
# This is a dirty and shameful workaround to get around the requirement for having
# Source tag poiting to the url with archived source code. Since in Jenkins we have the sources available
# and, what is even worse, the sources being built are not available in any kind of repository,
# we can't put anything meaningful in here. So the workaround consists in pointing to an empty tarball,
# which gets extracted without any effect.
Source0:        https://raw.githubusercontent.com/pszostek/vertica-hyperloglog/master/SPECS/empty.tar.gz
BuildRoot:      %{_tmppath}/%{name}-%{version}

BuildRequires:  gcc-c++
BuildRequires:  make
BuildRequires:  cmake
BuildRequires:  openssh
BuildRequires:  dialog

# this assumes that vertica is available in a yum repository
# this is the case at Criteo, because Jeremy Mauro put it there
BuildRequires:  vertica
Requires:       vertica

%description
For complete documentation, see the project home page on GitHub under http://github.com/criteo/vertica-hyperloglog.

%prep
%setup -q # -c %{name}-%{version}

%build
%cmake -DSDK_HOME='/opt/vertica/sdk' -DBUILD_TESTS=ON /builddir/build/SOURCES
make %{?_smp_mflags}

%install
%make_install # we just rely on the `install' target of make

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(755, root, root, 755)
/opt/vertica/lib/libhll.so

%check
make check # this runs the unit tests

%changelog
