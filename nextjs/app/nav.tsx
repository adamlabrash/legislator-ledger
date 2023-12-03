import Navbar from './navbar';

export default async function Nav() {
  const session = {};
  return <Navbar user={session} />;
}
